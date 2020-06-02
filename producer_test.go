// +build integration

package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProducerConfigValidate(t *testing.T) {
	conf := &ProducerConfig{
		Topic: "test-topic-pulsar-go",
	}

	err := conf.Validate()
	assert.Nil(t, err)
}
