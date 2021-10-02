package pulsar

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"

	pb "github.com/cornelk/pulsar-go/proto"
	"google.golang.org/protobuf/proto"
)

const (
	// maxFrameSize is the maximum size that Pulsar allows for messages.
	maxFrameSize        = 5 * 1024 * 1024
	magicCrc32c  uint16 = 0x0e01
)

var (
	crcOnce  sync.Once // guards init of crcTable via newConn
	crcTable *crc32.Table
)

// conn represents a connection to a Pulsar broker.
// The functions are not safe for concurrent use.
type conn struct {
	log Logger

	closer     io.Closer
	reader     bufio.Reader
	writer     bufio.Writer
	writeMutex sync.Mutex // protects writer writing
}

type clientConn interface {
	SendCallbackCommand(req *requests, reqID uint64, cmd proto.Message, callbacks ...requestCallback) error
	WriteCommand(cmd proto.Message, payload []byte) error
}

type brokerConnection struct {
	ctx  context.Context
	log  Logger
	conn clientConn
	req  *requests
}

// ErrNetClosing is returned when a network descriptor is used after
// it has been closed.
var ErrNetClosing = errors.New("use of closed network connection")

// newConn returns a new Pulsar broker connection.
func newConn(log Logger, con io.ReadWriteCloser) *conn {
	crcOnce.Do(func() {
		crcTable = crc32.MakeTable(crc32.Castagnoli)
	})
	return &conn{
		log:    log,
		closer: con,
		reader: *bufio.NewReader(con),
		writer: *bufio.NewWriter(con),
	}
}

// close the connection.
// Any blocked Read or Write operations will be unblocked and return errors.
func (c *conn) close() error {
	return c.closer.Close()
}

// WriteCommand sends a command to the Pulsar broker.
func (c *conn) WriteCommand(cmd proto.Message, payload []byte) error {
	c.log.Printf("*** Sending command: %+v", cmd)

	serialized, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshalling failed: %w", err)
	}

	cmdSize := uint32(len(serialized))

	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	b := make([]byte, 4)
	// write size of the frame, counting everything that comes after it
	binary.BigEndian.PutUint32(b, cmdSize+4+uint32(len(payload)))
	if _, err = c.writer.Write(b); err != nil {
		return fmt.Errorf("writing frame size failed: %w", err)
	}

	// write size of the protobuf-serialized command
	binary.BigEndian.PutUint32(b, cmdSize)
	if _, err = c.writer.Write(b); err != nil {
		return fmt.Errorf("writing command size failed: %w", err)
	}

	// write the protobuf-serialized command
	if _, err = c.writer.Write(serialized); err != nil {
		return fmt.Errorf("writing marshalled command failed: %w", err)
	}

	if len(payload) > 0 {
		// write the payload
		if _, err = c.writer.Write(payload); err != nil {
			return fmt.Errorf("writing command payload failed: %w", err)
		}
	}

	if err = c.writer.Flush(); err != nil {
		return fmt.Errorf("flushing connection failed: %w", err)
	}
	return nil
}

// readCommand reads a command and returns the optional payload size that can
// be read after the command.
func (c *conn) readCommand() (*command, error) {
	b := make([]byte, 4+4)
	_, err := io.ReadFull(&c.reader, b)
	if err != nil {
		if e, ok := err.(*net.OpError); ok && e.Err.Error() == ErrNetClosing.Error() {
			return nil, ErrNetClosing
		}

		return nil, fmt.Errorf("reading header failed: %w", err)
	}

	// 4 byte totalSize
	frameSize := binary.BigEndian.Uint32(b)
	if frameSize > maxFrameSize {
		return nil, fmt.Errorf("frame size exceeds maximum: %d", frameSize)
	}

	// 4 byte commandSize
	cmdSize := binary.BigEndian.Uint32(b[4:])

	// read commandSize bytes of message
	data := make([]byte, cmdSize)
	_, err = io.ReadFull(&c.reader, data)
	if err != nil {
		return nil, fmt.Errorf("reading body failed: %w", err)
	}

	cmd := &command{BaseCommand: &pb.BaseCommand{}}
	if err = proto.Unmarshal(data, cmd.BaseCommand); err != nil {
		return nil, fmt.Errorf("unmarshalling failed: %w", err)
	}

	cmd.payloadSize = frameSize - cmdSize - 4
	return cmd, nil
}

// readMessageMetaData reads the message metadata with the given payload
// size that has been returned from command header.
func (c *conn) readMessageMetaData(payloadSize uint32) (msgMeta *pb.MessageMetadata, payload []byte, err error) {
	if payloadSize < 10 || payloadSize > maxFrameSize {
		return nil, nil, fmt.Errorf("invalid payload size %d", payloadSize)
	}

	b := make([]byte, payloadSize)
	_, err = io.ReadFull(&c.reader, b)
	if err != nil {
		return nil, nil, fmt.Errorf("reading data failed: %w", err)
	}

	// 2-byte byte array (0x0e01) identifying the current format
	magicNumber := binary.BigEndian.Uint16(b)
	if magicNumber != magicCrc32c {
		return nil, nil, errors.New("header does not contain magic")
	}

	// CRC32-C checksum of size and payload
	checksum := binary.BigEndian.Uint32(b[2:6])
	computedChecksum := crc32.Checksum(b[2+4:], crcTable)
	if checksum != computedChecksum {
		return nil, nil, errors.New("checksum mismatch")
	}

	// size of the message metadata
	size := binary.BigEndian.Uint32(b[6:10])
	if size == 0 || size > maxFrameSize {
		return nil, nil, fmt.Errorf("invalid message metadata size %d", size)
	}

	msgMeta = &pb.MessageMetadata{}
	if err = proto.Unmarshal(b[10:10+size], msgMeta); err != nil {
		return nil, nil, fmt.Errorf("unmarshalling failed: %w", err)
	}

	return msgMeta, b[10+size:], nil
}

// readBatchedMessage reads a batched message from the given payload buffer.
func (c *conn) readBatchedMessage(b []byte) (meta *pb.SingleMessageMetadata, msg, remaining []byte, err error) {
	size := binary.BigEndian.Uint32(b)
	if int(size) > len(b)-4 {
		return nil, nil, nil,
			fmt.Errorf("message size %d exceeds buffer length %d", size, len(b)-4)
	}

	meta = &pb.SingleMessageMetadata{}
	if err = proto.Unmarshal(b[4:4+size], meta); err != nil {
		return nil, nil, nil, fmt.Errorf("unmarshalling failed: %w", err)
	}

	ps := uint32(meta.GetPayloadSize())
	end := 4 + size + ps
	return meta, b[4+size : end], b[end:], nil
}

// SendCallbackCommand sends a command to the server that accepts callbacks.
// It will execute all optional callback handlers.
// The function returns after the server responded to the command.
func (c *conn) SendCallbackCommand(req *requests, reqID uint64, cmd proto.Message, callbacks ...requestCallback) error {
	// The server response will be processed asynchronously by the client.
	// In order to block this function until we have received the response and
	// executed all callbacks, we use a channel. The channel capacity is 1,
	// so we never block the response handling for other messages.
	callbackErr := make(chan error, 1)
	req.addCallback(reqID, func(resp *command) (err error) {
		defer func() { callbackErr <- err }()

		for _, callback := range callbacks {
			if err := callback(resp); err != nil {
				return err
			}
		}

		return resp.err
	})

	if err := c.WriteCommand(cmd, nil); err != nil {
		return err
	}

	return <-callbackErr
}
