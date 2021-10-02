package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestMessageIDMarshal(t *testing.T) {
	m1 := MessageID{
		LedgerId:  proto.Uint64(1),
		EntryId:   proto.Uint64(2),
		Partition: proto.Int32(3),
	}

	b, err := m1.Marshal()
	require.NoError(t, err)

	var m2 MessageID
	err = m2.Unmarshal(b)
	require.NoError(t, err)

	assert.Equal(t, *m1.LedgerId, *m2.LedgerId)
	assert.Equal(t, *m1.EntryId, *m2.EntryId)
	assert.Equal(t, *m1.Partition, *m2.Partition)
}
