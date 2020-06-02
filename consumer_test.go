package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsumerConfigValidate(t *testing.T) {
	conf := &ConsumerConfig{
		Topic: randomTopicName(),
	}

	err := conf.Validate()
	assert.Nil(t, err)
}
