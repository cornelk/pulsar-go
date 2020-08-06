package pulsar

import (
	"errors"
	"fmt"
	"net/url"

	pb "github.com/cornelk/pulsar-go/proto"
	"github.com/golang/protobuf/proto"
)

type command struct {
	*pb.BaseCommand
	payloadSize uint32 // may be zero
	err         error  // TODO: currently errors are logged multiple times (in callbacks and on client level)
	custom      string // TODO: improve name
}

type (
	commandHandler func(cmd *command) error
	commands       map[pb.BaseCommand_Type]commandHandler
)

// noRequestID is used inside this library to signal that a command has no request ID.
const noRequestID = 0

func (c *Client) newCommandMap() commands {
	return commands{
		pb.BaseCommand_CONNECTED:                        c.handleConnectedCommand,
		pb.BaseCommand_SEND_RECEIPT:                     c.handleSendReceiptCommand,
		pb.BaseCommand_SEND_ERROR:                       c.handleSendErrorCommand,
		pb.BaseCommand_MESSAGE:                          c.handleMessage,
		pb.BaseCommand_SUCCESS:                          c.handleSuccessCommand,
		pb.BaseCommand_ERROR:                            c.handleErrorCommand,
		pb.BaseCommand_CLOSE_CONSUMER:                   c.handleCloseConsumerCommand,
		pb.BaseCommand_PRODUCER_SUCCESS:                 c.handleProducerSuccessCommand,
		pb.BaseCommand_PING:                             c.handlePingCommand,
		pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:    c.handlePartitionedMetadataResponseCommand,
		pb.BaseCommand_LOOKUP_RESPONSE:                  c.handleTopicLookupResponse,
		pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:     c.handleLastMessageIDResponse,
		pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE: c.handleGetTopicsOfNamespaceResponse,
	}
}

// TODO: write unit test to proof that for every handled command we have a way to determine the request ID.
func (cmd *command) requestID() uint64 {
	switch cmd.GetType() {
	case pb.BaseCommand_CONNECTED, pb.BaseCommand_MESSAGE, pb.BaseCommand_PING:
		return noRequestID
	case pb.BaseCommand_SEND_RECEIPT:
		return cmd.GetSendReceipt().GetSequenceId()
	case pb.BaseCommand_SEND_ERROR:
		return cmd.GetSendError().GetSequenceId()
	case pb.BaseCommand_SUCCESS:
		return cmd.GetSuccess().GetRequestId()
	case pb.BaseCommand_ERROR:
		return cmd.GetError().GetRequestId()
	case pb.BaseCommand_CLOSE_CONSUMER:
		return cmd.GetCloseConsumer().GetRequestId()
	case pb.BaseCommand_PRODUCER_SUCCESS:
		return cmd.GetProducerSuccess().GetRequestId()
	case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		return cmd.GetPartitionMetadataResponse().GetRequestId()
	case pb.BaseCommand_LOOKUP_RESPONSE:
		return cmd.GetLookupTopicResponse().GetRequestId()
	case pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
		return cmd.GetGetLastMessageIdResponse().GetRequestId()
	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
		return cmd.GetGetTopicsOfNamespaceResponse().GetRequestId()
	default:
		return noRequestID
	}
}

func (c *Client) handleRequestCallback(cmd *command) error {
	reqID := cmd.requestID()
	if reqID == noRequestID {
		return nil
	}

	callback, custom := c.req.remove(reqID)
	if callback == nil {
		return nil
	}

	cmd.custom = custom
	return callback(cmd)
}

func (c *Client) handleConnectedCommand(*command) error {
	close(c.connected)
	return nil
}

func (c *Client) handleTopicLookupResponse(cmd *command) error {
	resp := cmd.LookupTopicResponse
	if r := resp.GetResponse(); r != pb.CommandLookupTopicResponse_Connect {
		// TODO support redirect
		return fmt.Errorf("topic lookup response not supported: %v", r)
	}

	// TODO callback in case of error?
	u, err := url.Parse(resp.GetBrokerServiceUrl())
	if err != nil {
		return fmt.Errorf("parsing URL: %w", err)
	}

	if c.host != u.Host { // is same server that client is already connected to?
		_ = u // TODO support
	}

	return c.handleRequestCallback(cmd)
}

func (c *Client) handleMessage(base *command) error {
	cmd := base.Message
	msgMeta, payload, err := c.conn.readMessageMetaData(base.payloadSize)
	if err != nil {
		return err
	}
	if msgMeta.GetCompression() != pb.CompressionType_NONE {
		return fmt.Errorf("compressed messages not supported") // TODO support
	}

	consumerID := cmd.GetConsumerId()
	cons, ok := c.consumers.get(consumerID)
	if !ok {
		return errors.New("consumer of message not found")
	}

	id := cmd.MessageId
	if num := msgMeta.GetNumMessagesInBatch(); num > 0 {
		var msg []byte
		for i := int32(0); i < num; i++ {
			_, msg, payload, err = c.conn.readBatchedMessage(payload)
			if err != nil {
				return err
			}
			m := &Message{
				consumerID: consumerID,
				Body:       msg,
				Topic:      cons.topic,
				ID: &MessageID{
					LedgerId:   id.LedgerId,
					EntryId:    id.EntryId,
					Partition:  id.Partition,
					BatchIndex: id.BatchIndex,
				},
			}
			cons.incomingMessages <- m
		}
	} else {
		m := &Message{
			Body: payload,
			ID:   (*MessageID)(id),
		}
		cons.incomingMessages <- m
	}

	// getting a single message or batch counts as 1 permit
	return cons.useMessagePermit()
}

func (c *Client) handleSendReceiptCommand(base *command) error {
	cmd := base.SendReceipt
	prod, ok := c.producers.get(cmd.GetProducerId())
	if !ok {
		return errors.New("producer not found")
	}

	seq := cmd.GetSequenceId()
	return prod.processSendResult(seq, cmd.MessageId, nil)
}

func (c *Client) handleSendErrorCommand(base *command) error {
	cmd := base.SendError
	prod, ok := c.producers.get(cmd.GetProducerId())
	if !ok {
		return errors.New("producer not found")
	}

	err := fmt.Errorf("%s: %s", cmd.GetError(), cmd.GetMessage())
	seq := cmd.GetSequenceId()
	return prod.processSendResult(seq, nil, err)
}

func (c *Client) handleSuccessCommand(cmd *command) error {
	return c.handleRequestCallback(cmd)
}

func (c *Client) handleErrorCommand(cmd *command) error {
	cmd.err = fmt.Errorf("%s: %s", cmd.Error.GetError(), cmd.Error.GetMessage())
	return c.handleRequestCallback(cmd)
}

func (c *Client) handleProducerSuccessCommand(cmd *command) error {
	return c.handleRequestCallback(cmd)
}

func (c *Client) handlePingCommand(*command) error {
	return sendPongCommand(c.conn)
}

func (c *Client) handlePartitionedMetadataResponseCommand(cmd *command) error {
	return c.handleRequestCallback(cmd)
}

func (c *Client) handleLastMessageIDResponse(cmd *command) error {
	return c.handleRequestCallback(cmd)
}

func (c *Client) handleGetTopicsOfNamespaceResponse(cmd *command) error {
	return c.handleRequestCallback(cmd)
}

func sendConnectCommand(c clientConn) error {
	base := &pb.BaseCommand{
		Type: pb.BaseCommand_CONNECT.Enum(),
		Connect: &pb.CommandConnect{
			ClientVersion:   proto.String("Pulsar Go " + libraryVersion),
			AuthMethodName:  proto.String(""),
			ProtocolVersion: proto.Int32(protocolVersion),
		},
	}
	return c.WriteCommand(base, nil)
}

func sendPongCommand(c clientConn) error {
	base := &pb.BaseCommand{
		Type: pb.BaseCommand_PONG.Enum(),
		Pong: &pb.CommandPong{},
	}
	return c.WriteCommand(base, nil)
}

func newCloseConsumerCommand(consumerID, requestID uint64) *pb.BaseCommand {
	return &pb.BaseCommand{
		Type: pb.BaseCommand_CLOSE_CONSUMER.Enum(),
		CloseConsumer: &pb.CommandCloseConsumer{
			ConsumerId: proto.Uint64(consumerID),
			RequestId:  proto.Uint64(requestID),
		},
	}
}

func newCloseProducerCommand(producerID, requestID uint64) *pb.BaseCommand {
	return &pb.BaseCommand{
		Type: pb.BaseCommand_CLOSE_PRODUCER.Enum(),
		CloseProducer: &pb.CommandCloseProducer{
			ProducerId: proto.Uint64(producerID),
			RequestId:  proto.Uint64(requestID),
		},
	}
}

func newGetTopicsOfNamespaceCommand(requestID uint64, namespace string) *pb.BaseCommand {
	return &pb.BaseCommand{
		Type: pb.BaseCommand_GET_TOPICS_OF_NAMESPACE.Enum(),
		GetTopicsOfNamespace: &pb.CommandGetTopicsOfNamespace{
			RequestId: proto.Uint64(requestID),
			Namespace: proto.String(publicTenant + "/" + namespace),
			Mode:      pb.CommandGetTopicsOfNamespace_PERSISTENT.Enum(),
		},
	}
}

func newGetLastMessageIDCommand(consumerID, requestID uint64) *pb.BaseCommand {
	return &pb.BaseCommand{
		Type: pb.BaseCommand_GET_LAST_MESSAGE_ID.Enum(),
		GetLastMessageId: &pb.CommandGetLastMessageId{
			ConsumerId: proto.Uint64(consumerID),
			RequestId:  proto.Uint64(requestID),
		},
	}
}

// The server sent a close consumer command. assign the Consumer a new ID
// and reconnect it.
func (c *Client) handleCloseConsumerCommand(base *command) error {
	cmd := base.CloseConsumer

	consumerID := cmd.GetConsumerId()
	cons, ok := c.consumers.get(consumerID)
	if !ok {
		return fmt.Errorf("consumer %d not found", consumerID)
	}

	c.consumers.delete(consumerID)
	newID := c.consumers.newID()
	c.consumers.add(newID, cons)

	cons.clearReceiverQueue()
	cons.reset(newID)

	if cons.multi != nil {
		cons.multi.changeConsumerID(cons, consumerID, newID)
	}

	go c.topicLookup(cons.topic, cons.topicLookupFinished)
	return nil
}
