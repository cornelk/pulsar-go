//go:build integration

package pulsar

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newTestProducer(t *testing.T, client *Client, topic string) (*Producer, string) {
	t.Helper()
	if topic == "" {
		topic = randomTopicName()
	}
	prodConf := ProducerConfig{
		Topic:     topic,
		Name:      "test-producer",
		BatchSize: 1,
	}

	ctx := context.Background()
	var err error
	producer, err := client.NewProducer(ctx, prodConf)
	require.NoError(t, err)
	return producer, topic
}

func sendMessage(t *testing.T, producer *Producer, s string) *Message {
	t.Helper()
	m := &Message{
		Body: []byte(s),
	}
	var err error
	ctx := context.Background()
	id, err := producer.WriteMessage(ctx, m.Body)
	if id.BatchIndex == nil {
		id.BatchIndex = proto.Int32(0)
	}
	require.NoError(t, err)
	require.NotNil(t, id)
	m.ID = id
	return m
}

func sendMessageAsync(t *testing.T, producer *Producer, s string) *Message {
	t.Helper()
	m := &Message{
		Body: []byte(s),
	}
	ctx := context.Background()
	require.Nil(t, producer.WriteMessageAsync(ctx, m.Body))
	return m
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
	require.NoError(t, err)
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
	require.NoError(t, err)
	assert.NotEmpty(t, prod.name)
}

func TestProducerBatchSize(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	topic := randomTopicName()
	prodConf := ProducerConfig{
		Topic:        topic,
		Name:         "test-producer",
		BatchSize:    2,
		BatchTimeout: time.Minute,
	}

	ctx := context.Background()
	prod, err := client.NewProducer(ctx, prodConf)
	require.NoError(t, err)

	consConf := ConsumerConfig{
		Topic:           topic,
		InitialPosition: EarliestPosition,
	}

	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	msg1 := sendMessageAsync(t, prod, "hello world 1")
	msg2 := sendMessageAsync(t, prod, "hello world 2")

	// wait for message to be available
	timeout := time.After(1 * time.Second)
	<-timeout
	assert.True(t, consumer.HasNext())

	readMessageAndCompare(t, consumer, msg1)
	readMessageAndCompare(t, consumer, msg2)
}

func TestProducerBatchTimeout(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	topic := randomTopicName()
	prodConf := ProducerConfig{
		Topic:        topic,
		Name:         "test-producer",
		BatchSize:    100,
		BatchTimeout: 500 * time.Millisecond,
	}

	ctx := context.Background()

	consConf := ConsumerConfig{
		Topic:              topic,
		InitialPosition:    EarliestPosition,
		ForceTopicCreation: true,
	}

	// start consumer before producer to make for better test timing
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	prod, err := client.NewProducer(ctx, prodConf)
	require.NoError(t, err)

	msg1 := sendMessageAsync(t, prod, "hello world 1")
	msg2 := sendMessageAsync(t, prod, "hello world 2")

	// wait for message to be available
	timeout := time.After(1100 * time.Millisecond)
	<-timeout
	assert.True(t, consumer.HasNext())

	readMessageAndCompare(t, consumer, msg1)
	readMessageAndCompare(t, consumer, msg2)
}
