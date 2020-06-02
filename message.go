package pulsar

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	pb "github.com/cornelk/pulsar-go/proto"
	"github.com/golang/protobuf/proto"
)

// Message is a data structure representing Pulsar messages.
type Message struct {
	consumerID uint64 // used to identify the consumer on Ack
	Body       []byte
	ID         *MessageID
}

// MessageID contains the ID of a message.
type MessageID pb.MessageIdData

// Marshal the ID.
func (id *MessageID) Marshal() ([]byte, error) {
	return proto.Marshal((*pb.MessageIdData)(id))
}

// Unmarshal the ID.
func (id *MessageID) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, (*pb.MessageIdData)(id))
}

func getMessageMetaData(crcTable *crc32.Table, meta *pb.MessageMetadata, payload []byte) ([]byte, error) {
	serializedMeta, err := proto.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshalling failed: %w", err)
	}

	size := uint32(len(serializedMeta))
	headerContentSize := 2 + 4 + 4 + int(size)
	b := make([]byte, headerContentSize+len(payload))

	// 2-byte byte array (0x0e01) identifying the current format
	binary.BigEndian.PutUint16(b, magicCrc32c)

	// size of the protobuf-serialized meta data
	binary.BigEndian.PutUint32(b[2+4:], size)

	// serialized meta data
	copy(b[2+4+4:], serializedMeta)

	// messages payload
	copy(b[headerContentSize:], payload)

	checksum := crc32.Checksum(b[2+4:], crcTable)
	// CRC32-C checksum of size and payload
	binary.BigEndian.PutUint32(b[2:], checksum)

	return b, nil
}

func getBatchedMessagePayload(meta *pb.SingleMessageMetadata, msg *Message) ([]byte, error) {
	serialized, err := proto.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshalling failed: %w", err)
	}

	b := make([]byte, 4+len(serialized)+len(msg.Body))
	binary.BigEndian.PutUint32(b, uint32(len(serialized)))
	copy(b[4:], serialized)
	copy(b[4+len(serialized):], msg.Body)

	return b, nil
}
