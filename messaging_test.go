// +build integration

package pulsar

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) *Client {
	client, err := NewClient("pulsar://localhost:6650", WithLogger(newTestLogger(t)))
	require.Nil(t, err)

	ctx := context.Background()
	err = client.Dial(ctx)
	require.Nil(t, err)
	return client
}

func readMessageAndCompare(t *testing.T, consumer Consumer, expected *Message) *Message {
	ctx := context.Background()
	m, err := consumer.ReadMessage(ctx)
	require.Nil(t, err)
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
		assert.Nil(t, client.Close())
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
	require.Nil(t, err)

	m := readMessageAndCompare(t, consumer, msg1)
	topicDetail, err := NewTopic(m.Topic)
	require.Nil(t, err)
	assert.Equal(t, topic, topicDetail.LocalName)

	err = consumer.AckMessage(m)
	require.Nil(t, err)

	// restart consumer
	err = consumer.Close()
	require.Nil(t, err)
	consumer, err = client.NewConsumer(ctx, consConf)
	require.Nil(t, err)

	readMessageAndCompare(t, consumer, msg2)
}

func TestSendReceiveLatestPositionExclusive(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
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
	require.Nil(t, err)

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
		assert.Nil(t, client.Close())
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
	require.Nil(t, err)

	// wait for message to be available
	timeout := time.After(1 * time.Second)
	<-timeout
	require.True(t, consumer.HasNext())

	readMessageAndCompare(t, consumer, msg)
}

func TestEmptyTopicLatestPositionInclusive(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
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
	require.Nil(t, err)

	// wait for message to be available
	timeout := time.After(1 * time.Second)
	<-timeout
	assert.False(t, consumer.HasNext())
}

func TestConsumerNonExistingTopic(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	consConf := ConsumerConfig{
		Topic: randomTopicName(),
	}

	ctx := context.Background()
	_, err := client.NewConsumer(ctx, consConf)
	expected := fmt.Sprintf("TopicNotFound: Topic persistent://public/default/%s does not exist", consConf.Topic)
	assert.Equal(t, expected, err.Error())
}

func TestNothingToReceive(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
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
	require.Nil(t, err)

	timeout := time.After(1 * time.Second)
	done := make(chan error)
	go func() {
		_, err := consumer.ReadMessage(ctx)
		done <- err
	}()

	select {
	case <-timeout:
	case err := <-done:
		assert.Error(t, err)
		t.Fail()
	}
	assert.False(t, consumer.HasNext())
}

func TestSeek(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
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
	require.Nil(t, err)

	readMessageAndCompare(t, consumer, msg1)

	readMessageAndCompare(t, consumer, msg2)

	err = consumer.SeekMessage(msg1)
	require.Nil(t, err)

	readMessageAndCompare(t, consumer, msg1)
}

func TestConsumerTopicPattern(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	topic := randomTopicName()

	producer1, _ := newTestProducer(t, client, topic+"-1")
	msg1 := sendMessage(t, producer1, "hello world 1")

	producer2, _ := newTestProducer(t, client, topic+"-2")
	msg2 := sendMessage(t, producer2, "hello world 2")

	consConf := ConsumerConfig{
		TopicPattern:    topic + "-.*",
		InitialPosition: EarliestPosition,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.Nil(t, err)

	m1, err := consumer.ReadMessage(ctx)
	require.Nil(t, err)
	require.NotNil(t, m1)
	m2, err := consumer.ReadMessage(ctx)
	require.Nil(t, err)
	require.NotNil(t, m2)

	assert.Nil(t, consumer.AckMessage(m1))
	assert.Nil(t, consumer.AckMessage(m2))

	var topic1Msg, topic2Msg *Message

	// messages are returned in random order
	if string(m1.Body) == string(msg1.Body) {
		topic1Msg = m1
		topic2Msg = m2
	} else {
		topic1Msg = m2
		topic2Msg = m1
	}

	assert.Equal(t, topic1Msg.Body, msg1.Body)
	assert.Equal(t, topic2Msg.Body, msg2.Body)

	t1, err := NewTopic(topic1Msg.Topic)
	require.Nil(t, err)
	assert.Equal(t, topic+"-1", t1.LocalName)

	t2, err := NewTopic(topic2Msg.Topic)
	require.Nil(t, err)
	assert.NotNil(t, t2)
	assert.Equal(t, topic+"-2", t2.LocalName)
}

func TestConsumerTopicPatternDiscovery(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	topic := randomTopicName()

	consConf := ConsumerConfig{
		TopicPattern:                  topic + "-.*",
		TopicPatternDiscoveryInterval: 500,
		InitialPosition:               EarliestPosition,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.Nil(t, err)

	time.Sleep(time.Second)

	producer, _ := newTestProducer(t, client, topic+"-1")
	msg := sendMessage(t, producer, "hello world")

	m, err := consumer.ReadMessage(ctx)
	require.Nil(t, err)
	require.NotNil(t, m)

	assert.Nil(t, consumer.AckMessage(m))
	assert.Equal(t, msg.Body, m.Body)
}

func TestGetLastMessageID(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
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
	require.Nil(t, err)

	msgid, err := consumer.LastMessageID()
	require.Nil(t, err)
	require.NotNil(t, msgid)
	assert.True(t, *msgid.EntryId == math.MaxUint64)

	sendMessage(t, producer, "hello world")
	msgid, err = consumer.LastMessageID()
	require.Nil(t, err)
	require.NotNil(t, msgid)
	assert.EqualValues(t, 0, *msgid.EntryId)
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
	require.Nil(t, err)

	// Read and ack the available messages for this consumer.
	for i := 0; i < messageCount; i++ {
		m := readMessageAndCompare(t, consumer, messages[i])
		require.NoError(t, consumer.AckMessage(m))
	}
}
