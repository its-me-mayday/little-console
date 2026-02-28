package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

type Product struct {
	ProductID   string `json:"product_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Supplier    string `json:"supplier"`
	Customer    string `json:"customer"`
}

type CloudEvent struct {
	SpecVersion     string  `json:"specversion"`
	ID              string  `json:"id"`
	Type            string  `json:"type"`
	Source          string  `json:"source"`
	Time            string  `json:"time"`
	DataContentType string  `json:"datacontenttype"`
	Data            Product `json:"data"`
}

func main() {
	fmt.Println("Start Consumer")

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		panic("KAFKA_BROKERS not set")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers},
		Topic:   "test-topic",
		GroupID: "products-consumer-group",
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		fmt.Println("Shutdown received")
		cancel()
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			fmt.Println("Error:", err)
			continue
		}

		var event CloudEvent
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			fmt.Println("Invalid JSON:", err)
			continue
		}

		fmt.Printf("Event received: %s\n", event.Type)
		fmt.Printf("Product ID: %s\n", event.Data.ProductID)
		fmt.Printf("Name: %s\n", event.Data.Name)
		fmt.Printf("Supplier: %s\n", event.Data.Supplier)
		fmt.Printf("Customer: %s\n", event.Data.Customer)
		fmt.Println("------")
	}
}
