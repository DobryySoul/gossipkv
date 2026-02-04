package main

import (
	"context"
	"fmt"
	"log"

	"github.com/DobryySoul/gossipkv"
)

func main() {
	db, err := gossipkv.New[string, string](
		gossipkv.WithCodec(gossipkv.StringCodec{}),
	)
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	if err := db.Set(context.Background(), "hello", "world"); err != nil {
		log.Fatalf("set: %v", err)
	}

	value, err := db.Get(context.Background(), "hello")
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	fmt.Println("hello:", value)
}
