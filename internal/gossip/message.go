package gossip

import (
	"bytes"
	"encoding/gob"
	"time"
)

const (
	msgDigest = "digest"
	msgDelta  = "delta"
	msgNeed   = "need"
)

type Message struct {
	Kind    string
	Digest  []DigestItem
	Records []Record
	Need    []string
}

type DigestItem struct {
	Key       string
	Version   uint64
	NodeID    string
	UpdatedAt time.Time
}

type Record struct {
	Key       string
	Value     []byte
	Version   uint64
	NodeID    string
	UpdatedAt time.Time
}

func encodeMessage(msg Message) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(msg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeMessage(data []byte) (Message, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var msg Message
	if err := dec.Decode(&msg); err != nil {
		return Message{}, err
	}
	return msg, nil
}
