package pulsar

import (
	"context"
	"errors"
	"io"
	"regexp"
)

type multiTopicConsumer struct {
	ctx context.Context

	topicPattern     *regexp.Regexp
	incomingMessages chan *Message
	consumers        *consumerRegistry
	closer           consumerCloser
}

func newMultiTopicConsumer(closer consumerCloser, conn brokerConnection, config ConsumerConfig) (*multiTopicConsumer, error) {
	c := &multiTopicConsumer{
		ctx:       conn.ctx,
		consumers: newConsumerRegistry(),
		closer:    closer,
	}

	if config.MessageChannel == nil {
		c.incomingMessages = make(chan *Message, 1000)
	} else {
		c.incomingMessages = config.MessageChannel
	}

	var err error
	c.topicPattern, err = regexp.Compile(config.TopicPattern)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *multiTopicConsumer) addConsumer(id uint64, consumer *consumer) {
	c.consumers.add(id, consumer)
}

func (c *multiTopicConsumer) changeConsumerID(consumer *consumer, oldID, newID uint64) {
	c.consumers.add(newID, consumer)
	c.consumers.delete(oldID)
}

func (c *multiTopicConsumer) Close() error {
	c.consumers.mu.RLock()
	defer c.consumers.mu.RLock()

	for _, con := range c.consumers.consumers {
		_ = c.closer.CloseConsumer(con.consumerID)
	}

	return nil
}

func (c *multiTopicConsumer) ReadMessage(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, c.ctx.Err()

	case m, ok := <-c.incomingMessages:
		if !ok {
			return nil, io.EOF
		}

		return m, nil
	}
}

func (c *multiTopicConsumer) SeekMessage(_ *Message) error {
	return errors.New("seek not supported in multi topic consumer")
}

func (c *multiTopicConsumer) AckMessage(msg *Message) error {
	consumer, ok := c.consumers.get(msg.consumerID)
	if !ok {
		return errors.New("consumer of message not found")
	}

	return consumer.AckMessage(msg)
}

func (c *multiTopicConsumer) HasNext() bool {
	return len(c.incomingMessages) > 0
}

func (c *multiTopicConsumer) LastMessageID() (*MessageID, error) {
	return nil, errors.New("last message id not supported in multi topic consumer")
}
