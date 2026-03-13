package gossipkv

import (
	"context"
	"errors"
	"testing"
)

func TestBytesCodecZeroCopy(t *testing.T) {
	codec := BytesCodec{}
	original := []byte("hello world")

	// Test Marshal
	marshaled, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(original) > 0 && &marshaled[0] != &original[0] {
		t.Fatalf("Marshal created a deep copy")
	}

	// Test Unmarshal
	unmarshaled, err := codec.Unmarshal(marshaled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(marshaled) > 0 && &unmarshaled[0] != &marshaled[0] {
		t.Fatalf("Unmarshal created a deep copy")
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
