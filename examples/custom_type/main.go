package main

import (
	"context"
	"fmt"
	"log"

	"github.com/DobryySoul/gossipkv"
)

type Profile struct {
	Name  string
	Email string
}

func main() {
	db, err := gossipkv.New[string, Profile]()
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	profile := Profile{Name: "Nikita", Email: "nikita@example.com"}
	if err := db.Set(context.Background(), "user:1", profile); err != nil {
		log.Fatalf("set: %v", err)
	}

	value, err := db.Get(context.Background(), "user:1")
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	fmt.Printf("profile: %+v\n", value)
}
