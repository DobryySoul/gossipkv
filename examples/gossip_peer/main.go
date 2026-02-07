package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DobryySoul/gossipkv"
)

func main() {
	var (
		nodeID   = flag.String("id", "node-a", "node id")
		bindAddr = flag.String("bind", "127.0.0.1:9001", "bind address")
		seeds    = flag.String("seeds", "", "comma-separated peers")
	)
	flag.Parse()

	var peers []string
	if *seeds != "" {
		peers = strings.Split(*seeds, ",")
	}

	db, err := gossipkv.New[string, string](
		gossipkv.WithNodeID(*nodeID),
		gossipkv.WithBindAddr(*bindAddr),
		gossipkv.WithSeeds(peers),
		gossipkv.WithGossipInterval(500*time.Millisecond),
		gossipkv.WithCodec(gossipkv.StringCodec{}),
	)
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	key := fmt.Sprintf("hello:%s", *nodeID)
	if err := db.Set(context.Background(), key, "hi from "+*nodeID); err != nil {
		log.Fatalf("set: %v", err)
	}

	fmt.Printf("node %s running on %s\n", *nodeID, *bindAddr)
	fmt.Println("press Ctrl+C to exit")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("\nshutting down...")
}
