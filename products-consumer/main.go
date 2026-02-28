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
	fmt.Println("Product Consumer Started")

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

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signals
		cancel()
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			fmt.Println("Read error:", err)
			continue
		}

		var event CloudEvent
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			fmt.Println("Invalid JSON:", err)
			continue
		}

		switch event.Type {
		case "product.created":
			handleProductCreated(event)
		default:
			fmt.Printf("Ignored event type: %s\n", event.Type)
		}
	}
}

func handleProductCreated(event CloudEvent) {
	p := event.Data

	fmt.Println("Processing product.created event")
	fmt.Printf("Event ID: %s\n", event.ID)
	fmt.Printf("Product ID: %s\n", p.ProductID)
	fmt.Printf("Name: %s\n", p.Name)
	fmt.Printf("Description: %s\n", p.Description)
	fmt.Printf("Supplier: %s\n", p.Supplier)
	fmt.Printf("Customer: %s\n", p.Customer)
	fmt.Println("-----")
}
