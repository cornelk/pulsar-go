// +build integration

package pulsar

import (
	"context"
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

func sendMessage(t *testing.T, producer *Producer, s string) *Message {
	m := &Message{
		Body: []byte(s),
	}
	var err error
	ctx := context.Background()
	id, err := producer.WriteMessage(ctx, m.Body)
	require.Nil(t, err)
	require.NotNil(t, id)
	m.ID = id
	return m
}

func readMessageAndCompare(t *testing.T, consumer Consumer, expected *Message) *Message {
	ctx := context.Background()
	m, err := consumer.ReadMessage(ctx)
	require.Nil(t, err)
	require.NotNil(t, m)
	assert.Equal(t, expected.Body, m.Body)
	assert.Equal(t, expected.ID.LedgerId, m.ID.LedgerId)
	assert.Equal(t, expected.ID.EntryId, m.ID.EntryId)
	assert.Equal(t, expected.ID.Partition, m.ID.Partition)
	assert.Equal(t, expected.ID.BatchIndex, m.ID.BatchIndex)
	return m
}

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
	assert.Equal(t, topicDetail.LocalName, topic)

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
	assert.Equal(t, err.Error(), "TopicNotFound: Topic does not exist")
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
	assert.Equal(t, t1.LocalName, topic+"-1")

	t2, err := NewTopic(topic2Msg.Topic)
	require.Nil(t, err)
	assert.NotNil(t, t2)
	assert.Equal(t, t2.LocalName, topic+"-2")
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
	assert.Equal(t, m.Body, msg.Body)
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
	assert.EqualValues(t, *msgid.EntryId, 0)
}
