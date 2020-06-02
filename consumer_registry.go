package pulsar

import (
	"sync"
	"sync/atomic"
)

type consumerRegistry struct {
	consumerIDs uint64
	mu          sync.RWMutex
	consumers   map[uint64]*consumer
}

func newConsumerRegistry() *consumerRegistry {
	return &consumerRegistry{
		mu:        sync.RWMutex{},
		consumers: map[uint64]*consumer{},
	}
}

func (r *consumerRegistry) newID() uint64 {
	id := atomic.AddUint64(&r.consumerIDs, 1)
	return id
}

func (r *consumerRegistry) add(id uint64, consumer *consumer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.consumers[id] = consumer
}

func (r *consumerRegistry) delete(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.consumers, id)
}

func (r *consumerRegistry) get(id uint64) (consumer *consumer, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	consumer, ok = r.consumers[id]
	return consumer, ok
}

func (r *consumerRegistry) getAndDelete(id uint64) (consumer *consumer, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	consumer, ok = r.consumers[id]
	delete(r.consumers, id)
	return consumer, ok
}

func (r *consumerRegistry) all() []*consumer {
	r.mu.Lock()
	defer r.mu.Unlock()

	consumers := make([]*consumer, 0, len(r.consumers))
	for _, cons := range r.consumers {
		consumers = append(consumers, cons)
	}
	return consumers
}
