//go:build integration

package pulsar

import (
	"context"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) *Client {
	t.Helper()
	client, err := NewClient("pulsar://localhost:6650", WithLogger(newTestLogger(t)))
	require.NoError(t, err)

	// make sure that the namespace exists to avoid the error from pulsar:
	// "Policies not found for public/default namespace".
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		"http://localhost:8080/admin/v2/namespaces/public/default", nil)
	require.NoError(t, err)
	h := &http.Client{}
	resp, err := h.Do(req)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)

	err = client.Dial(ctx)
	require.NoError(t, err)
	return client
}

func readMessageAndCompare(t *testing.T, consumer Consumer, expected *Message) *Message {
	t.Helper()
	ctx := context.Background()
	m, err := consumer.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, expected.Body, m.Body)

	if expected.ID != nil {
		assert.Equal(t, expected.ID.LedgerId, m.ID.LedgerId)
		assert.Equal(t, expected.ID.EntryId, m.ID.EntryId)
		assert.Equal(t, expected.ID.Partition, m.ID.Partition)
		assert.Equal(t, expected.ID.BatchIndex, m.ID.BatchIndex)
	}
	return m
}

func TestSendReceiveEarliestPosition(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	producer, topic := newTestProducer(t, client, "")

	msg1 := sendMessage(t, producer, "hello world 1")
	msg2 := sendMessage(t, producer, "hello world 2")

	consConf := ConsumerConfig{
		Topic:           topic,
		Subscription:    "test-sub",
		InitialPosition: EarliestPosition,
		Durable:         true,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	m := readMessageAndCompare(t, consumer, msg1)
	topicDetail, err := NewTopic(m.Topic)
	require.NoError(t, err)
	assert.Equal(t, topic, topicDetail.LocalName)

	err = consumer.AckMessage(m)
	require.NoError(t, err)

	// restart consumer
	err = consumer.Close()
	require.NoError(t, err)
	consumer, err = client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	readMessageAndCompare(t, consumer, msg2)
}

func TestSendReceiveLatestPositionExclusive(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	producer, topic := newTestProducer(t, client, "")

	sendMessage(t, producer, "hello world 1")

	consConf := ConsumerConfig{
		Topic:           topic,
		Subscription:    "test-sub",
		InitialPosition: LatestPosition,
		Durable:         true,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	msg2 := sendMessage(t, producer, "hello world 2")

	// wait for message to be available
	timeout := time.After(1 * time.Second)
	<-timeout
	assert.True(t, consumer.HasNext())

	readMessageAndCompare(t, consumer, msg2)
}

func TestSendReceiveLatestPositionInclusive(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	producer, topic := newTestProducer(t, client, "")
	msg := sendMessage(t, producer, "hello world 1")

	consConf := ConsumerConfig{
		Topic:                   topic,
		Subscription:            "test-sub",
		InitialPosition:         LatestPosition,
		StartMessageIDInclusive: true,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	// wait for message to be available
	timeout := time.After(1 * time.Second)
	<-timeout
	require.True(t, consumer.HasNext())

	readMessageAndCompare(t, consumer, msg)
}

func TestConsumerEmptyTopicLatestPositionInclusive(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	consConf := ConsumerConfig{
		Topic:                   randomTopicName(),
		Subscription:            "test-sub",
		InitialPosition:         LatestPosition,
		StartMessageIDInclusive: true,
		ForceTopicCreation:      true,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	// wait for message to be available
	timeout := time.After(1 * time.Second)
	<-timeout
	assert.False(t, consumer.HasNext())
}

func TestConsumerNonExistingTopic(t *testing.T) {
	t.SkipNow() // TODO fix, use error streaming

	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	consConf := ConsumerConfig{
		Topic: randomTopicName(),
	}

	ctx := context.Background()
	_, err := client.NewConsumer(ctx, consConf)
	assert.True(t, strings.HasPrefix(err.Error(), "TopicNotFound:"))
}

func TestConsumerNothingToReceive(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	topic := randomTopicName()
	consConf := ConsumerConfig{
		Topic:              topic,
		Subscription:       "test-sub",
		InitialPosition:    LatestPosition,
		Durable:            true,
		ForceTopicCreation: true,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	timeout := time.After(1 * time.Second)
	done := make(chan error)
	go func() {
		_, err := consumer.ReadMessage(ctx)
		done <- err
	}()

	select {
	case <-timeout:
	case err := <-done:
		require.Error(t, err)
		t.Fail()
	}
	assert.False(t, consumer.HasNext())
}

func TestConsumerSeek(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	producer, topic := newTestProducer(t, client, "")

	msg1 := sendMessage(t, producer, "hello world 1")
	msg2 := sendMessage(t, producer, "hello world 2")
	sendMessage(t, producer, "hello world 3")

	consConf := ConsumerConfig{
		Topic:           topic,
		Subscription:    "test-sub",
		Type:            SharedSubscription,
		InitialPosition: EarliestPosition,
		Durable:         false,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	readMessageAndCompare(t, consumer, msg1)

	readMessageAndCompare(t, consumer, msg2)

	err = consumer.SeekMessage(msg1)
	require.NoError(t, err)

	readMessageAndCompare(t, consumer, msg1)
}

func TestGetLastMessageID(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	producer, topic := newTestProducer(t, client, "")

	consConf := ConsumerConfig{
		Topic:           topic,
		Subscription:    "test-sub",
		Type:            ExclusiveSubscription,
		InitialPosition: EarliestPosition,
		Durable:         true,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	messageID, err := consumer.LastMessageID()
	require.NoError(t, err)
	require.NotNil(t, messageID)
	assert.Equal(t, *messageID.EntryId, uint64(math.MaxUint64))

	sendMessage(t, producer, "hello world")
	messageID, err = consumer.LastMessageID()
	require.NoError(t, err)
	require.NotNil(t, messageID)
	assert.EqualValues(t, 0, *messageID.EntryId)
}

func TestConsumer_ReceiverQueueSize(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	producer, topic := newTestProducer(t, client, "")

	receiverQueueSize := 1000
	messageCount := receiverQueueSize * 3
	messages := make([]*Message, messageCount)

	// Publish an extra message over the configured queue size.
	for i := 0; i < messageCount; i++ {
		messages[i] = sendMessage(t, producer, "hello world")
	}

	// Create an exclusive consumer for the topic.
	consConf := ConsumerConfig{
		Topic:           topic,
		Subscription:    "test-sub",
		InitialPosition: EarliestPosition,
		Durable:         true,
		MessageChannel:  make(chan *Message, receiverQueueSize),
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	// Read and ack the available messages for this consumer.
	for i := 0; i < messageCount; i++ {
		m := readMessageAndCompare(t, consumer, messages[i])
		require.NoError(t, consumer.AckMessage(m))
	}
}
