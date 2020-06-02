package pulsar

import (
	"math"
	"math/rand"
	"strings"
	"time"

	pb "github.com/cornelk/pulsar-go/proto"
	"github.com/golang/protobuf/proto"
)

var earliestMessageID = &pb.MessageIdData{
	LedgerId:  proto.Uint64(math.MaxUint64),
	EntryId:   proto.Uint64(math.MaxUint64),
	Partition: proto.Int32(-1),
}
var latestMessageID = &pb.MessageIdData{
	LedgerId:  proto.Uint64(math.MaxInt64),
	EntryId:   proto.Uint64(math.MaxInt64),
	Partition: proto.Int32(-1),
}

// randomString returns a random string of n characters.
func randomString(src rand.Source, n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)

	sb := strings.Builder{}
	sb.Grow(n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

func randomConsumerName() string {
	rnd := rand.NewSource(time.Now().UnixNano())
	s := "consumer-" + randomString(rnd, 8)
	return s
}

func randomSubscriptionName() string {
	rnd := rand.NewSource(time.Now().UnixNano())
	s := "sub-" + randomString(rnd, 8)
	return s
}

func randomTopicName() string {
	rnd := rand.NewSource(time.Now().UnixNano())
	s := "topic-" + randomString(rnd, 8)
	return s
}
