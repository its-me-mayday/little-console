package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("Start Consumer")

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		panic("KAFKA_BROKERS not set")
	}

	topic := "test-topic"
	groupID := "products-consumer-group"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers},
		Topic:   topic,
		GroupID: groupID,
	})

	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		fmt.Println("Received shutdown")
		cancel()
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("Terminate consumer")
				break
			}
			fmt.Println("Error:", err)
			continue
		}

	fmt.Printf("Key=%s Value=%s\n", string(msg.Key), string(msg.Value))

	for _, h := range msg.Headers {
		fmt.Printf("Header: %s=%s\n", h.Key, string(h.Value))
	}
	}
}
