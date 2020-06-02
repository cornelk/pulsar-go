package pulsar

import "sync/atomic"

type sequencer struct {
	id uint64
}

// generates a new ID and returns it as a pointer, as needed for use
// in the proto messages.
func (s *sequencer) newID() uint64 {
	return atomic.AddUint64(&s.id, 1)
}
