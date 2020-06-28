package pulsar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
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

	// The name of the producer.
	Name string

	// Limit on how many messages will be buffered before being sent as a batch.
	//
	// The default is a batch size of 100 messages.
	BatchSize int

	// Time limit on how often a batch that is not full yet will be flushed and
	// sent to Pulsar.
	//
	// The default is to flush every second.
	BatchTimeout time.Duration

	// Capacity of the internal producer message queue.
	//
	// The default is to use a queue capacity of 1000 messages.
	QueueCapacity int
}

const (
	defaultBatchSize     = 100
	defaultBatchTimeout  = time.Second
	defaultQueueCapacity = 1000
)

type producerCloser interface {
	CloseProducer(producerID uint64) error
}

// Producer provides a high-level API for sending messages to Pulsar.
type Producer struct {
	ctx  context.Context
	log  Logger
	conn clientConn
	req  *requests

	topic        *Topic
	name         *string
	batchSize    int
	batchTimeout time.Duration

	producerID      uint64
	pendingMessages chan *syncMessage
	sequenceID      uint64
	sendResults     map[uint64][]*sendResult
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
	if config.BatchSize < 0 || config.BatchSize > math.MaxInt32 {
		return errors.New("invalid batch size")
	}
	if config.QueueCapacity < 0 {
		return errors.New("invalid queue capacity")
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

		topic:        topic,
		name:         proto.String(config.Name),
		batchSize:    config.BatchSize,
		batchTimeout: config.BatchTimeout,

		producerID:  producerID,
		sendResults: map[uint64][]*sendResult{},

		crcTable:  crc32.MakeTable(crc32.Castagnoli),
		connected: make(chan struct{}, 1),
		closer:    closer,
	}

	queueCapacity := config.QueueCapacity
	if queueCapacity == 0 {
		queueCapacity = defaultQueueCapacity
	}
	p.pendingMessages = make(chan *syncMessage, queueCapacity)

	if p.batchSize == 0 {
		p.batchSize = defaultBatchSize
	}
	if p.batchTimeout.Nanoseconds() == 0 {
		p.batchTimeout = defaultBatchTimeout
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

// WriteMessage puts the message into the message queue, blocks until the
// message has been sent and an acknowledgement message is received from
// Pulsar.
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

// WriteMessageAsync puts the message into the message queue.
// If the message queue is full, this function will block until it can write
// to the queue. The queue size can be specified in the Producer options.
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

func (p *Producer) sendMessageCommand(batch []*syncMessage) error {
	seq := proto.Uint64(p.sequenceID)
	base := &pb.BaseCommand{
		Type: pb.BaseCommand_SEND.Enum(),
		Send: &pb.CommandSend{
			ProducerId:  proto.Uint64(p.producerID),
			SequenceId:  seq,
			NumMessages: proto.Int32(int32(len(batch))),
		},
	}
	p.sequenceID++

	p.addBatchToSendResults(batch, *seq)

	now := proto.Uint64(uint64(time.Now().UnixNano() / int64(time.Millisecond)))

	var messagePayload bytes.Buffer
	for _, msg := range batch {
		meta := &pb.SingleMessageMetadata{
			PayloadSize: proto.Int32(int32(len(msg.msg.Body))),
			EventTime:   now,
		}

		b, err := getBatchedMessagePayload(meta, msg.msg)
		if err != nil {
			return err
		}
		if _, err = messagePayload.Write(b); err != nil {
			return err
		}
	}

	msgMeta := &pb.MessageMetadata{
		ProducerName:       p.name,
		SequenceId:         seq,
		PublishTime:        now,
		Compression:        pb.CompressionType_NONE.Enum(),
		UncompressedSize:   proto.Uint32(uint32(messagePayload.Len())),
		NumMessagesInBatch: proto.Int32(int32(len(batch))),
	}

	payload, err := getMessageMetaData(p.crcTable, msgMeta, messagePayload.Bytes())
	if err != nil {
		return err
	}

	return p.conn.WriteCommand(base, payload)
}

// addBatchToSendResults adds the batch to the send results callback if any
// message of the batch wants to know results in a callback channel.
func (p *Producer) addBatchToSendResults(batch []*syncMessage, sequenceID uint64) {
	sendResults := make([]*sendResult, len(batch))
	var sendResultCallbacks int

	for i, msg := range batch {
		if msg.res != nil {
			sendResults[i] = msg.res
			sendResultCallbacks++
		}
	}

	if sendResultCallbacks == 0 {
		return
	}

	p.sendResultsLock.Lock()
	p.sendResults[sequenceID] = sendResults
	p.sendResultsLock.Unlock()
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
	var (
		batch = make([]*syncMessage, 0, p.batchSize)
	)

	timer := time.NewTimer(p.batchTimeout)
	defer timer.Stop()
	timerRunning := true

	for {
		mustFlush := false

		// reset the timer to tick in the given time interval after last send
		if timerRunning && !timer.Stop() {
			<-timer.C
		}
		timer.Reset(p.batchTimeout)
		timerRunning = true

		select {
		case <-p.ctx.Done():
			return

		case m, ok := <-p.pendingMessages:
			if !ok {
				return
			}

			batch = append(batch, m)
			mustFlush = len(batch) >= p.batchSize

		case <-timer.C:
			timerRunning = false

			if len(batch) > 0 {
				mustFlush = true
			}
		}

		if !mustFlush {
			continue
		}

		if err := p.sendMessageCommand(batch); err != nil {
			p.log.Printf("Sending message command failed: %w", err)
		}
		batch = batch[:0]
	}
}

func (p *Producer) processSendResult(sequenceID uint64, id *pb.MessageIdData, err error) error {
	p.sendResultsLock.Lock()
	results, ok := p.sendResults[sequenceID]
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

	for _, res := range results {
		if res == nil {
			continue
		}

		res.id = id
		res.err = err

		close(res.ch)
	}

	return nil
}
