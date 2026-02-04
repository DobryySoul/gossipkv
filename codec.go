package gossipkv

import (
	"bytes"
	"encoding/gob"
)

// Codec serializes values for transport between nodes.
type Codec[V any] interface {
	Marshal(value V) ([]byte, error)
	Unmarshal(data []byte) (V, error)
}

// BytesCodec passes through raw bytes without copying.
// The caller must not modify the returned slice.
type BytesCodec struct{}

func (BytesCodec) Marshal(value []byte) ([]byte, error) {
	return value, nil
}

func (BytesCodec) Unmarshal(data []byte) ([]byte, error) {
	return data, nil
}

// StringCodec encodes strings as raw bytes.
// It allocates on marshal/unmarshal to keep data immutable.
type StringCodec struct{}

func (StringCodec) Marshal(value string) ([]byte, error) {
	return []byte(value), nil
}

func (StringCodec) Unmarshal(data []byte) (string, error) {
	return string(data), nil
}

// GobCodec uses encoding/gob for serialization.
// It works with most Go types without extra registration.
type GobCodec[V any] struct{}

func (GobCodec[V]) Marshal(value V) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (GobCodec[V]) Unmarshal(data []byte) (V, error) {
	var value V
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&value); err != nil {
		return value, err
	}
	return value, nil
}
