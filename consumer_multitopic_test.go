// +build integration

package pulsar

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	require.NoError(t, err)

	m1, err := consumer.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)
	m2, err := consumer.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)

	assert.Nil(t, consumer.AckMessage(m1))
	assert.Nil(t, consumer.AckMessage(m2))

	messages := []string{string(m1.Body), string(m2.Body)}
	m := map[string]*Message{
		string(m1.Body): m1,
		string(m2.Body): m2,
	}
	sort.Strings(messages)
	assert.Equal(t, []string{string(msg1.Body), string(msg2.Body)}, messages)

	t1, err := NewTopic(m[messages[0]].Topic)
	require.NoError(t, err)
	assert.Equal(t, topic+"-1", t1.LocalName)

	t2, err := NewTopic(m[messages[1]].Topic)
	require.NoError(t, err)
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
	require.NoError(t, err)

	time.Sleep(time.Second)

	producer, _ := newTestProducer(t, client, topic+"-1")
	msg := sendMessage(t, producer, "hello world")

	m, err := consumer.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)

	assert.Nil(t, consumer.AckMessage(m))
	assert.Equal(t, msg.Body, m.Body)
}

func TestConsumerTopicPatternInitialPosition(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	topic := randomTopicName()

	cb := func(topic string) (position InitialPosition, StartMessageID []byte, err error) {
		if strings.HasSuffix(topic, "1") {
			return EarliestPosition, nil, nil
		}
		return LatestPosition, nil, nil
	}

	consConf := ConsumerConfig{
		TopicPattern:                  topic + "-.*",
		TopicPatternDiscoveryInterval: 500,
		InitialPositionCallback:       cb,
		StartMessageIDInclusive:       true,
	}

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, consConf)
	require.NoError(t, err)

	time.Sleep(time.Second)

	producer1, _ := newTestProducer(t, client, topic+"-1")
	sendMessage(t, producer1, "hello world 1a")
	sendMessage(t, producer1, "hello world 1b")
	producer2, _ := newTestProducer(t, client, topic+"-2")
	sendMessage(t, producer2, "hello world 2a")
	sendMessage(t, producer2, "hello world 2b")

	m1, err := consumer.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)
	assert.Nil(t, consumer.AckMessage(m1))

	m2, err := consumer.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)
	assert.Nil(t, consumer.AckMessage(m2))

	m3, err := consumer.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotNil(t, m3)
	assert.Nil(t, consumer.AckMessage(m3))

	messages := []string{string(m1.Body), string(m2.Body), string(m3.Body)}
	sort.Strings(messages)
	assert.Equal(t, []string{"hello world 1a", "hello world 1b", "hello world 2b"}, messages)
}
