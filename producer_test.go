// +build integration

package pulsar

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProducer(t *testing.T, client *Client, topic string) (*Producer, string) {
	if topic == "" {
		topic = randomTopicName()
	}
	prodConf := ProducerConfig{
		Topic: topic,
		Name:  "test-producer",
	}

	ctx := context.Background()
	var err error
	producer, err := client.NewProducer(ctx, prodConf)
	require.Nil(t, err)
	return producer, topic
}

func TestProducerConfigValidate(t *testing.T) {
	conf := &ProducerConfig{
		Topic: "test-topic-pulsar-go",
	}

	err := conf.Validate()
	assert.Nil(t, err)
}

func TestProducerRestartSequence(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	prod, topic := newTestProducer(t, client, "")

	m1 := sendMessage(t, prod, "hello world 1")
	assert.EqualValues(t, 0, *m1.ID.EntryId)

	m2 := sendMessage(t, prod, "hello world 2")
	assert.EqualValues(t, 1, *m2.ID.EntryId)

	// restart producer
	err := prod.Close()
	require.Nil(t, err)
	prod, _ = newTestProducer(t, client, topic)
	assert.EqualValues(t, 0, prod.sequenceID)

	m3 := sendMessage(t, prod, "hello world 3")
	assert.EqualValues(t, 2, *m3.ID.EntryId)
	assert.Equal(t, *m1.ID.LedgerId, *m3.ID.LedgerId)
}

func TestProducerBrokerGeneratedName(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	prodConf := ProducerConfig{
		Topic: randomTopicName(),
	}

	ctx := context.Background()
	prod, err := client.NewProducer(ctx, prodConf)
	require.Nil(t, err)
	assert.NotEmpty(t, prod.name)
}
