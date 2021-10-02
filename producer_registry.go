package pulsar

import (
	"sync"
	"sync/atomic"
)

type producerRegistry struct {
	producerIDs  uint64
	producersMtx sync.RWMutex
	producers    map[uint64]*Producer
}

func newProducerRegistry() *producerRegistry {
	return &producerRegistry{
		producersMtx: sync.RWMutex{},
		producers:    map[uint64]*Producer{},
	}
}

func (r *producerRegistry) newID() uint64 {
	id := atomic.AddUint64(&r.producerIDs, 1)
	return id
}

func (r *producerRegistry) add(id uint64, producer *Producer) {
	r.producersMtx.Lock()
	r.producers[id] = producer
	r.producersMtx.Unlock()
}

func (r *producerRegistry) get(id uint64) (producer *Producer, ok bool) {
	r.producersMtx.RLock()
	producer, ok = r.producers[id]
	r.producersMtx.RUnlock()
	return producer, ok
}

func (r *producerRegistry) getAndDelete(id uint64) (producer *Producer, ok bool) {
	r.producersMtx.Lock()
	producer, ok = r.producers[id]
	if ok {
		delete(r.producers, id)
	}
	r.producersMtx.Unlock()
	return producer, ok
}

func (r *producerRegistry) all() []*Producer {
	r.producersMtx.RLock()
	producers := make([]*Producer, 0, len(r.producers))
	for _, prod := range r.producers {
		producers = append(producers, prod)
	}
	defer r.producersMtx.RUnlock()
	return producers
}
