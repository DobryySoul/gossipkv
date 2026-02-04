package main

import (
	"context"
	"fmt"
	"log"

	"github.com/DobryySoul/gossipkv"
)

func main() {
	db, err := gossipkv.New[string, []byte](
		gossipkv.WithCodec(gossipkv.BytesCodec{}),
	)
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	if err := db.Set(context.Background(), "payload", []byte("hello")); err != nil {
		log.Fatalf("set: %v", err)
	}

	value, err := db.Get(context.Background(), "payload")
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	fmt.Println("payload:", string(value))
}
