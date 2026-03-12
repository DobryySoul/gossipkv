package gossipkv

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestBytesCodecDeepCopy(t *testing.T) {
	codec := BytesCodec{}
	original := []byte("hello world")

	// Test Marshal
	marshaled, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if &marshaled[0] == &original[0] {
		t.Fatalf("Marshal did not create a deep copy")
	}

	// Test Unmarshal
	unmarshaled, err := codec.Unmarshal(marshaled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if &unmarshaled[0] == &marshaled[0] {
		t.Fatalf("Unmarshal did not create a deep copy")
	}

	// Verify that modifying the original doesn't change the marshaled or unmarshaled values
	original[0] = 'X'
	if bytes.Equal(marshaled, original) {
		t.Fatalf("modifying original modified marshaled bytes")
	}
	if bytes.Equal(unmarshaled, original) {
		t.Fatalf("modifying original modified unmarshaled bytes")
	}
}

func TestDBCheckContextCancellation(t *testing.T) {
	db, err := New[string, string]()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	err = db.Set(ctx, "k1", "v1")
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("expected ErrCanceled, got %v", err)
	}

	_, err = db.Get(ctx, "k1")
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("expected ErrCanceled, got %v", err)
	}
}

func TestDBClose(t *testing.T) {
	db, err := New[string, string]()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = db.Close(t.Context())
	if err != nil {
		t.Fatalf("unexpected error on first close: %v", err)
	}

	err = db.Close(t.Context())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on second close, got %v", err)
	}

	err = db.Set(t.Context(), "k1", "v1")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on Set after close, got %v", err)
	}
}
