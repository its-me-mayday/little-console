package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Product struct {
	ProductID   string `json:"product_id" bson:"product_id"`
	Name        string `json:"name" bson:"name"`
	Description string `json:"description" bson:"description"`
	Supplier    string `json:"supplier" bson:"supplier"`
	Customer    string `json:"customer" bson:"customer"`
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
	mongoURI := os.Getenv("MONGO_URL")

	if brokers == "" || mongoURI == "" {
		panic("Environment variables not set")
	}

	ctx := context.Background()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		panic(err)
	}


	collection := mongoClient.
		Database("productsdb").
		Collection("products", &options.CollectionOptions{
			WriteConcern: writeconcern.New(writeconcern.WMajority()),
		})

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers},
		Topic:   "test-topic",
		GroupID: "products-consumer-group",
	})
	defer reader.Close()
	
	dlqWriter := &kafka.Writer{
	Addr:     kafka.TCP(brokers),
	Topic:    "products.dlq",
	Balancer: &kafka.LeastBytes{},
}
defer dlqWriter.Close()

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
	sendToDLQ(ctx, dlqWriter, msg, "invalid_json")
	continue
}
		if event.Type == "product.created" {
			processProductEvent(ctx, collection, dlqWriter, event)
		}
	}
}

func processProductEvent(
	ctx context.Context,
	collection *mongo.Collection,
	dlqWriter *kafka.Writer,
	event CloudEvent,
) {
	doc := bson.M{
		"_id":         event.ID,
		"product_id":  event.Data.ProductID,
		"name":        event.Data.Name,
		"description": event.Data.Description,
		"supplier":    event.Data.Supplier,
		"customer":    event.Data.Customer,
		"created_at":  time.Now(),
	}

	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			fmt.Println("Duplicate event ignored:", event.ID)
			return
		}

		sendToDLQ(ctx, dlqWriter, kafka.Message{
			Key:   []byte(event.ID),
			Value: []byte("mongo_insert_error"),
		}, "mongo_error")

		return
	}

	fmt.Println("Product stored successfully:", event.Data.ProductID)
}
func sendToDLQ(ctx context.Context, writer *kafka.Writer, original kafka.Message, reason string) {
	dlqPayload := map[string]interface{}{
		"reason":  reason,
		"payload": string(original.Value),
	}

	bytes, _ := json.Marshal(dlqPayload)

	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   original.Key,
		Value: bytes,
	})
	if err != nil {
		fmt.Println("Failed to send to DLQ:", err)
		return
	}

	fmt.Println("Message sent to DLQ")
}
