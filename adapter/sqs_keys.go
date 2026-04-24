package adapter

import (
	"encoding/base64"
	"strings"

	"github.com/cockroachdb/errors"
)

// SQS keyspace prefixes. Kept in sync with the naming in
// docs/design/2026_04_24_proposed_sqs_compatible_adapter.md.
const (
	// SqsQueueMetaPrefix prefixes queue-metadata records.
	SqsQueueMetaPrefix = "!sqs|queue|meta|"
	// SqsQueueGenPrefix prefixes the per-queue monotonic generation counter.
	// Bumped on DeleteQueue / PurgeQueue so keys from an older incarnation of
	// the same queue name cannot leak into a newly created queue.
	SqsQueueGenPrefix = "!sqs|queue|gen|"
)

func sqsQueueMetaKey(queueName string) []byte {
	return []byte(SqsQueueMetaPrefix + encodeSQSSegment(queueName))
}

func sqsQueueGenKey(queueName string) []byte {
	return []byte(SqsQueueGenPrefix + encodeSQSSegment(queueName))
}

// encodeSQSSegment emits a printable, byte-ordered-unique representation of a
// queue name. Base64 raw URL encoding matches the encoding the DynamoDB
// adapter uses for table segments (see encodeDynamoSegment) so that operators
// reading raw keys from Pebble see the same shape across both adapters.
func encodeSQSSegment(v string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(v))
}

func decodeSQSSegment(v string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return string(b), nil
}

// queueNameFromMetaKey pulls the queue name out of a !sqs|queue|meta|<seg>
// key. The second return reports success so callers can skip keys that were
// not written by this adapter.
func queueNameFromMetaKey(key []byte) (string, bool) {
	enc, ok := strings.CutPrefix(string(key), SqsQueueMetaPrefix)
	if !ok || enc == "" {
		return "", false
	}
	name, err := decodeSQSSegment(enc)
	if err != nil {
		return "", false
	}
	return name, true
}
