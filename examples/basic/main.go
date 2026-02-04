package main

import (
	"context"
	"fmt"
	"log"

	"github.com/DobryySoul/gossipkv"
)

func main() {
	db, err := gossipkv.New[string, string]()
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	if err := db.Set(context.Background(), "version", "v1.0.0"); err != nil {
		log.Fatalf("set: %v", err)
	}

	value, err := db.Get(context.Background(), "version")
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	fmt.Println("version:", value)
}
