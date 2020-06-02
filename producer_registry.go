package pulsar

import (
	"sync"
	"sync/atomic"
)

type producerRegistry struct {
	producerIDs uint64
	mu          sync.RWMutex
	producers   map[uint64]*Producer
}

func newProducerRegistry() *producerRegistry {
	return &producerRegistry{
		mu:        sync.RWMutex{},
		producers: map[uint64]*Producer{},
	}
}

func (r *producerRegistry) newID() uint64 {
	id := atomic.AddUint64(&r.producerIDs, 1)
	return id
}

func (r *producerRegistry) add(id uint64, producer *Producer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.producers[id] = producer
}

func (r *producerRegistry) get(id uint64) (producer *Producer, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	producer, ok = r.producers[id]
	return producer, ok
}

func (r *producerRegistry) getAndDelete(id uint64) (producer *Producer, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	producer, ok = r.producers[id]
	delete(r.producers, id)
	return producer, ok
}

func (r *producerRegistry) all() []*Producer {
	r.mu.Lock()
	defer r.mu.Unlock()

	producers := make([]*Producer, 0, len(r.producers))
	for _, prod := range r.producers {
		producers = append(producers, prod)
	}
	return producers
}
