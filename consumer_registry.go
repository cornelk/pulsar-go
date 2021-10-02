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
	r.consumers[id] = consumer
	r.mu.Unlock()
}

func (r *consumerRegistry) delete(id uint64) {
	r.mu.Lock()
	delete(r.consumers, id)
	r.mu.Unlock()
}

func (r *consumerRegistry) get(id uint64) (consumer *consumer, ok bool) {
	r.mu.RLock()
	consumer, ok = r.consumers[id]
	r.mu.RUnlock()
	return consumer, ok
}

func (r *consumerRegistry) getAndDelete(id uint64) (consumer *consumer, ok bool) {
	r.mu.Lock()
	consumer, ok = r.consumers[id]
	if ok {
		delete(r.consumers, id)
	}
	r.mu.Unlock()
	return consumer, ok
}

func (r *consumerRegistry) all() []*consumer {
	r.mu.RLock()
	consumers := make([]*consumer, 0, len(r.consumers))
	for _, cons := range r.consumers {
		consumers = append(consumers, cons)
	}
	r.mu.RUnlock()
	return consumers
}
