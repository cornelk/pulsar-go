package pulsar

import (
	"sync"
)

type requestCallback func(*command) error

type requests struct {
	mutex     sync.RWMutex
	callbacks map[uint64]requestCallback
	custom    map[uint64]string
	sequencer *sequencer
}

func newRequests() *requests {
	return &requests{
		callbacks: map[uint64]requestCallback{},
		custom:    map[uint64]string{},
		sequencer: &sequencer{},
	}
}

// remove the request id and return it's assigned callback if existing.
func (r *requests) remove(reqID uint64) (requestCallback, string) {
	r.mutex.Lock()
	f, ok := r.callbacks[reqID]
	if !ok {
		r.mutex.Unlock()
		return nil, ""
	}

	delete(r.callbacks, reqID)
	s := r.custom[reqID]
	delete(r.custom, reqID)
	r.mutex.Unlock()
	return f, s
}

func (r *requests) newID() uint64 {
	return r.sequencer.newID()
}

func (r *requests) addCallback(reqID uint64, f requestCallback) {
	r.mutex.Lock()
	r.callbacks[reqID] = f
	r.mutex.Unlock()
}

func (r *requests) addCallbackCustom(reqID uint64, f requestCallback, custom string) {
	r.mutex.Lock()
	r.callbacks[reqID] = f
	r.custom[reqID] = custom
	r.mutex.Unlock()
}
