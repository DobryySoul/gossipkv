package gossipkv

import "errors"

var (
	// ErrNotFound indicates that the requested key is missing.
	ErrNotFound = errors.New("gossipkv: key not found")
	// ErrClosed indicates that the DB has been closed.
	ErrClosed = errors.New("gossipkv: db is closed")
	// ErrTimeout indicates that the context deadline expired.
	ErrTimeout = errors.New("gossipkv: operation timed out")
	// ErrCanceled indicates that the context was canceled.
	ErrCanceled = errors.New("gossipkv: operation canceled")
)
