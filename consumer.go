package pulsar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sync"

	pb "github.com/cornelk/pulsar-go/proto"
	"github.com/golang/protobuf/proto"
)

// SubscriptionType ...
type SubscriptionType pb.CommandSubscribe_SubType

// subscription type options
const (
	ExclusiveSubscription = SubscriptionType(pb.CommandSubscribe_Exclusive)
	SharedSubscription    = SubscriptionType(pb.CommandSubscribe_Shared)
	FailoverSubscription  = SubscriptionType(pb.CommandSubscribe_Failover)
	KeySharedSubscription = SubscriptionType(pb.CommandSubscribe_Key_Shared)
)

// InitialPosition ...
type InitialPosition pb.CommandSubscribe_InitialPosition

// subscription initial position options
const (
	// Start reading from the end topic, only getting messages published after the
	// reader was created.
	LatestPosition = InitialPosition(pb.CommandSubscribe_Latest)
	// Start reading from the earliest message available in the topic.
	EarliestPosition = InitialPosition(pb.CommandSubscribe_Earliest)
)

// ConsumerConfig is a configuration object used to create new instances
// of Consumer.
type ConsumerConfig struct {
	// The topic name to read messages from.
	Topic string

	// A regular expression for topics to read messages from.
	TopicPattern string

	// Interval in ms in which the client checks for topic changes
	// that match the set topic pattern and updates the subscriptions.
	// Default is 30000
	TopicPatternDiscoveryInterval int

	// A unique name for the subscription. If not specified, a random name
	// will be used.
	Subscription string

	// A unique name for the Consumer. If not specified, a random name
	// will be used.
	Name string

	// Select the subscription type to be used when subscribing to the topic.
	// Default is `Subscribe_Exclusive`
	Type SubscriptionType

	// Signal whether the subscription will initialize on latest
	// or earliest position.
	InitialPosition InitialPosition

	// If specified, the subscription will position the cursor
	// on the particular message id and will send messages from
	// that point.
	StartMessageID []byte

	// Include the message StartMessageID in the read messages.
	// If StartMessageID is not set but InitialPosition is set
	// to LatestPosition, the latest message ID of the topic
	// will be sent.
	StartMessageIDInclusive bool

	// Signal whether the subscription should be backed by a
	// durable cursor or not. For Readers, set to false, for
	// Consumers set Durable to true and specify a Subscription.
	// If Durable is true, StartMessageID will be ignored, as it
	// will be determined by the broker.
	Durable bool

	// MessageChannel sets a channel that receives all messages that the
	// consumer receives. If not set, a default channel for 1000 messages
	// will be created.
	MessageChannel chan *Message

	// If true, the subscribe operation will cause a topic to be
	// created if it does not exist already (and if topic auto-creation
	// is allowed by broker.
	// If false, the subscribe operation will fail if the topic
	// does not exist.
	ForceTopicCreation bool
}

// Consumer provides a high-level API for consuming messages from Pulsar.
type Consumer interface {
	// Close closes the subscription and unregisters from the Client.
	Close() error

	AckMessage(*Message) error
	// ReadMessage reads and return the next message from the Pulsar.
	ReadMessage(context.Context) (*Message, error)
	SeekMessage(*Message) error
	// HasNext returns whether there is a message available to read
	HasNext() bool

	// LastMessageID returns the last message ID of the topic.
	// If the topic is empty, EntryId will be math.MaxUint64
	LastMessageID() (*MessageID, error)
}

type consumerCloser interface {
	CloseConsumer(consumerID uint64) error
}

type consumerState int

const (
	consumerInit consumerState = iota
	consumerReady
	consumerSubscribed
	consumerClosing
	consumerClosed
)

type consumer struct {
	ctx  context.Context
	conn clientConn
	req  *requests

	// subscribe options
	topic                   string
	subscription            *string
	name                    *string
	subType                 pb.CommandSubscribe_SubType
	initialPosition         pb.CommandSubscribe_InitialPosition
	startMessageID          *pb.MessageIdData
	startMessageIDInclusive bool
	startMessageIDSeekDone  bool
	forceTopicCreation      *bool
	durable                 *bool
	incomingMessages        chan *Message

	consumerID uint64
	connected  chan error
	state      consumerState
	stateMu    sync.RWMutex
	closer     consumerCloser
	multi      *multiTopicConsumer // set if managed by a multi topic consumer
}

// Validate method validates the config properties.
func (config *ConsumerConfig) Validate() error {
	if config.Topic == "" && config.TopicPattern == "" {
		return errors.New("topic is not set")
	}
	if config.Topic != "" && config.TopicPattern != "" {
		return errors.New("topic and topic pattern are exclusive")
	}
	if config.StartMessageID != nil {
		id := &pb.MessageIdData{}
		if err := proto.Unmarshal(config.StartMessageID, id); err != nil {
			return fmt.Errorf("start message id unmarshalling: %w", err)
		}
	}
	if config.TopicPattern != "" {
		if _, err := regexp.Compile(config.TopicPattern); err != nil {
			return fmt.Errorf("topic pattern regular expression compiling: %w", err)
		}
		if config.TopicPatternDiscoveryInterval < 0 {
			return errors.New("invalid topic pattern discovery interval set")
		}
	}
	return nil
}

// newConsumer creates and returns a new Consumer configured with config.
func newConsumer(closer consumerCloser, conn brokerConnection, config ConsumerConfig, consumerID uint64) (*consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	c := &consumer{
		ctx:  conn.ctx,
		conn: conn.conn,
		req:  conn.req,

		topic:                   config.Topic,
		subType:                 pb.CommandSubscribe_SubType(config.Type),
		initialPosition:         pb.CommandSubscribe_InitialPosition(config.InitialPosition),
		startMessageIDInclusive: config.StartMessageIDInclusive,
		forceTopicCreation:      proto.Bool(config.ForceTopicCreation),
		durable:                 proto.Bool(config.Durable),

		consumerID: consumerID,
		connected:  make(chan error, 1),
		state:      consumerInit,
		closer:     closer,
	}

	if config.MessageChannel == nil {
		c.incomingMessages = make(chan *Message, 1000)
	} else {
		c.incomingMessages = config.MessageChannel
	}

	if !config.Durable {
		if config.StartMessageID != nil {
			id := &pb.MessageIdData{}
			if err := proto.Unmarshal(config.StartMessageID, id); err != nil {
				return nil, err
			}
			c.startMessageID = id
		} else {
			if config.InitialPosition == EarliestPosition {
				c.startMessageID = earliestMessageID
			} else {
				c.startMessageID = latestMessageID
			}
		}
	}

	if config.Name == "" {
		c.name = proto.String(randomConsumerName())
	} else {
		c.name = proto.String(config.Name)
	}
	if config.Subscription == "" {
		c.subscription = proto.String(randomSubscriptionName())
	} else {
		c.subscription = proto.String(config.Subscription)
	}

	return c, nil
}

func (c *consumer) topicLookupFinished(cmd *command) error {
	if cmd.err != nil {
		return cmd.err
	}

	id := c.req.newID()

	if c.startMessageIDInclusive && c.startMessageID == latestMessageID && !c.startMessageIDSeekDone {
		c.req.addCallback(id, c.getLastMessageID)
	} else {
		c.req.addCallback(id, c.sendFlowCommand)
	}

	c.stateMu.Lock()
	c.state = consumerReady
	c.stateMu.Unlock()

	return c.sendSubscribeCommand(cmd.custom, id)
}

func (c *consumer) getLastMessageID(base *command) error {
	if base.err != nil {
		c.connected <- base.err
		return base.err
	}

	reqID := c.req.newID()
	cmd := newGetLastMessageIDCommand(c.consumerID, reqID)
	c.req.addCallback(reqID, c.seekToLastMessageID)
	return c.conn.WriteCommand(cmd, nil)
}

func (c *consumer) seekToLastMessageID(cmd *command) error {
	c.startMessageIDSeekDone = true

	if cmd.err != nil {
		return cmd.err
	}
	if *cmd.GetLastMessageIdResponse.LastMessageId.EntryId == math.MaxUint64 {
		return c.sendFlowCommand(cmd) // empty topic
	}

	reqID := c.req.newID()
	return c.sendSeek(reqID, cmd.GetLastMessageIdResponse.LastMessageId)
}

func (c *consumer) Close() error {
	return c.closer.CloseConsumer(c.consumerID)
}

func (c *consumer) reset(newConsumerID uint64) {
	c.consumerID = newConsumerID
}

func (c *consumer) ReadMessage(ctx context.Context) (*Message, error) {
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

func (c *consumer) HasNext() bool {
	return len(c.incomingMessages) > 0
}

func (c *consumer) SeekMessage(msg *Message) error {
	id := &pb.MessageIdData{
		LedgerId: msg.ID.LedgerId,
		EntryId:  msg.ID.EntryId,
	}

	reqID := c.req.newID()

	ch := make(chan error, 1)
	c.req.addCallback(reqID, func(cmd *command) error {
		ch <- cmd.err
		return cmd.err
	})

	c.startMessageID = id
	if err := c.sendSeek(reqID, id); err != nil {
		return err
	}

	err := <-ch

	// the server issues a close consumer command, wait for the
	// automatic reconnection
	e := <-c.connected
	if err == nil {
		err = e
	}

	return err
}

func (c *consumer) sendSeek(reqID uint64, id *pb.MessageIdData) error {
	base := &pb.BaseCommand{
		Type: pb.BaseCommand_SEEK.Enum(),
		Seek: &pb.CommandSeek{
			ConsumerId: proto.Uint64(c.consumerID),
			RequestId:  proto.Uint64(reqID),
			MessageId:  id,
		},
	}
	return c.conn.WriteCommand(base, nil)
}

func (c *consumer) AckMessage(msg *Message) error {
	return c.sendAck(msg.ID)
}

func (c *consumer) sendAck(id *MessageID) error {
	base := &pb.BaseCommand{
		Type: pb.BaseCommand_ACK.Enum(),
		Ack: &pb.CommandAck{
			ConsumerId: proto.Uint64(c.consumerID),
			AckType:    pb.CommandAck_Individual.Enum(),
			MessageId:  []*pb.MessageIdData{(*pb.MessageIdData)(id)},
		},
	}
	return c.conn.WriteCommand(base, nil)
}

func (c *consumer) sendSubscribeCommand(topic string, reqID uint64) error {
	base := &pb.BaseCommand{
		Type: pb.BaseCommand_SUBSCRIBE.Enum(),
		Subscribe: &pb.CommandSubscribe{
			Topic:              &topic,
			Subscription:       c.subscription,
			SubType:            &c.subType,
			ConsumerId:         proto.Uint64(c.consumerID),
			RequestId:          proto.Uint64(reqID),
			ConsumerName:       c.name,
			Durable:            c.durable,
			StartMessageId:     c.startMessageID,
			ReadCompacted:      proto.Bool(false),
			InitialPosition:    &c.initialPosition,
			ForceTopicCreation: c.forceTopicCreation,
		},
	}
	return c.conn.WriteCommand(base, nil)
}

func (c *consumer) sendFlowCommand(cmd *command) error {
	defer func() {
		c.connected <- cmd.err
	}()
	if cmd.err != nil {
		return cmd.err
	}
	c.stateMu.Lock()
	c.state = consumerSubscribed
	c.stateMu.Unlock()

	base := &pb.BaseCommand{
		Type: pb.BaseCommand_FLOW.Enum(),
		Flow: &pb.CommandFlow{
			ConsumerId:     proto.Uint64(c.consumerID),
			MessagePermits: proto.Uint32(1000),
		},
	}
	return c.conn.WriteCommand(base, nil)
}

func (c *consumer) clearReceiverQueue() {
	for {
		select {
		case _, ok := <-c.incomingMessages:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func (c *consumer) LastMessageID() (*MessageID, error) {
	reqID := c.req.newID()
	cmd := newGetLastMessageIDCommand(c.consumerID, reqID)

	var msgID *MessageID
	respHandler := func(resp *command) error {
		if resp.err == nil && resp.GetLastMessageIdResponse != nil {
			msgID = (*MessageID)(resp.GetLastMessageIdResponse.LastMessageId)
		}
		return resp.err
	}

	err := c.conn.SendCallbackCommand(c.req, reqID, cmd, respHandler)
	if err != nil {
		return nil, err
	}

	return msgID, nil
}
