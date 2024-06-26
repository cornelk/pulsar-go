package pulsar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumerConfigValidate(t *testing.T) {
	conf := &ConsumerConfig{
		Topic: randomTopicName(),
	}

	err := conf.Validate()
	require.NoError(t, err)
}
