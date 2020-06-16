package pulsar

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	pb "github.com/cornelk/pulsar-go/proto"
	"github.com/golang/protobuf/proto"
)

// ProducerConfig is a configuration object used to create new instances
// of Producer.
type ProducerConfig struct {
	// The topic to write messages to.
	Topic string

	Name string
}

type producerCloser interface {
	CloseProducer(producerID uint64) error
}

// Producer provides a high-level API for sending messages to Pulsar.
type Producer struct {
	ctx  context.Context
	log  Logger
	conn clientConn
	req  *requests

	topic *Topic
	name  *string

	producerID      uint64
	pendingMessages chan *syncMessage
	sequenceID      uint64
	sendResults     map[uint64]*sendResult
	sendResultsLock sync.Mutex

	crcTable  *crc32.Table
	connected chan struct{}
	closer    producerCloser
}

type syncMessage struct {
	msg *Message
	res *sendResult
}

type sendResult struct {
	ch  chan struct{} // closing the channel marks this struct as filled
	err error
	id  *pb.MessageIdData
}

// Validate method validates the config properties.
func (config *ProducerConfig) Validate() error {
	if config.Topic == "" {
		return errors.New("topic is not set")
	}
	if _, err := NewTopic(config.Topic); err != nil {
		return fmt.Errorf("checking topic name: %w", err)
	}

	return nil
}

// newProducer creates and returns a new Producer configured with config.
func newProducer(closer producerCloser, conn brokerConnection, config ProducerConfig, producerID uint64) (*Producer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	topic, err := NewTopic(config.Topic)
	if err != nil {
		return nil, err
	}

	p := &Producer{
		ctx:  conn.ctx,
		log:  conn.log,
		conn: conn.conn,
		req:  conn.req,

		topic: topic,
		name:  proto.String(config.Name),

		producerID:      producerID,
		pendingMessages: make(chan *syncMessage, 1000),
		sendResults:     map[uint64]*sendResult{},

		crcTable:  crc32.MakeTable(crc32.Castagnoli),
		connected: make(chan struct{}, 1),
		closer:    closer,
	}

	return p, nil
}

func (p *Producer) topicReady(cmd *command) error {
	if cmd.err != nil {
		return cmd.err
	}

	reqID := p.req.newID()
	p.req.addCallback(reqID, p.handleProducerSuccess)
	return p.sendProduceCommand(reqID)
}

// Close stops writing messages and unregisters from the Client.
func (p *Producer) Close() error {
	return p.closer.CloseProducer(p.producerID)
}

// WriteMessage ...
func (p *Producer) WriteMessage(ctx context.Context, msg []byte) (*MessageID, error) {
	res := &sendResult{
		ch: make(chan struct{}),
	}
	m := &syncMessage{
		msg: &Message{
			Body: msg,
		},
		res: res,
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case p.pendingMessages <- m:
		<-res.ch
		return (*MessageID)(res.id), res.err
	}
}

// WriteMessageAsync ...
func (p *Producer) WriteMessageAsync(ctx context.Context, msg []byte) error {
	m := &syncMessage{
		msg: &Message{
			Body: msg,
		},
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.pendingMessages <- m:
		return nil
	}
}

func (p *Producer) sendMessageCommand(msg *syncMessage) error {
	seq := proto.Uint64(p.sequenceID)
	base := &pb.BaseCommand{
		Type: pb.BaseCommand_SEND.Enum(),
		Send: &pb.CommandSend{
			ProducerId:  proto.Uint64(p.producerID),
			SequenceId:  seq,
			NumMessages: proto.Int32(1),
		},
	}
	p.sequenceID++

	if msg.res != nil {
		p.sendResultsLock.Lock()
		p.sendResults[*seq] = msg.res
		p.sendResultsLock.Unlock()
	}

	now := proto.Uint64(uint64(time.Now().UnixNano() / int64(time.Millisecond)))
	meta := &pb.SingleMessageMetadata{
		PayloadSize: proto.Int32(int32(len(msg.msg.Body))),
		EventTime:   now,
	}

	var messagePayload []byte
	var err error
	if messagePayload, err = getBatchedMessagePayload(meta, msg.msg); err != nil {
		return err
	}

	msgMeta := &pb.MessageMetadata{
		ProducerName:       p.name,
		SequenceId:         seq,
		PublishTime:        now,
		UncompressedSize:   proto.Uint32(uint32(len(messagePayload))),
		NumMessagesInBatch: proto.Int32(1),
	}
	var payload []byte
	if payload, err = getMessageMetaData(p.crcTable, msgMeta, messagePayload); err != nil {
		return err
	}

	err = p.conn.WriteCommand(base, payload)
	return err
}

func (p *Producer) sendProduceCommand(reqID uint64) error {
	cmd := &pb.CommandProducer{
		Topic:      &p.topic.CompleteName,
		ProducerId: proto.Uint64(p.producerID),
		RequestId:  proto.Uint64(reqID),
	}
	if *p.name == "" {
		cmd.UserProvidedProducerName = proto.Bool(false)
	} else {
		cmd.ProducerName = p.name
		cmd.UserProvidedProducerName = proto.Bool(true)
	}

	base := &pb.BaseCommand{
		Type:     pb.BaseCommand_PRODUCER.Enum(),
		Producer: cmd,
	}
	return p.conn.WriteCommand(base, nil)
}

func (p *Producer) handleProducerSuccess(cmd *command) error {
	defer close(p.connected)
	if cmd.err != nil {
		return cmd.err
	}

	p.name = proto.String(cmd.ProducerSuccess.GetProducerName())
	p.sequenceID = uint64(cmd.ProducerSuccess.GetLastSequenceId() + 1)

	go p.messageProducerWorker()
	return nil
}

func (p *Producer) messageProducerWorker() {
	for {
		select {
		case <-p.ctx.Done():
			return

		case m, ok := <-p.pendingMessages:
			if !ok {
				return
			}

			if err := p.sendMessageCommand(m); err != nil {
				p.log.Printf("Sending message command failed: %w", err)
			}
		}
	}
}

func (p *Producer) processSendResult(sequenceID uint64, id *pb.MessageIdData, err error) error {
	p.sendResultsLock.Lock()
	res, ok := p.sendResults[sequenceID]
	delete(p.sendResults, sequenceID)
	p.sendResultsLock.Unlock()
	if !ok {
		return nil
	}

	if id != nil && id.Partition == nil {
		// set partition to -1 on unpartitioned topics for the id having the
		// same content as when returned from a subscription.
		id.Partition = proto.Int32(-1)
	}

	res.id = id
	res.err = err

	close(res.ch)
	return nil
}
