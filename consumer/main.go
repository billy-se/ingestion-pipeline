package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

type LogRecord struct {
	ID          int    `json:"id"`
	Timestamp   int64  `json:"timestamp"`
	User        string `json:"user"`
	Action      string `json:"action"`
	PayloadSize int    `json:"payload_size"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Warning: No .env found")
	}

	user := os.Getenv("user")
	if user == "" {
		user = "guest"
	}

	pass := os.Getenv("pass")
	if pass == "" {
		pass = "guest"
	}

	dsn := fmt.Sprintf("amqp://%s:%s@localhost:5672/", user, pass)

	conn, err := amqp.Dial(dsn)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"log_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(5, 0, false)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	log.Println(" [*] Worker is waiting for logs. To exit press CTRL+C")

	startTime := time.Now()
	totalProcessed := 0

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var batch []LogRecord
			err := json.Unmarshal(d.Body, &batch)
			if err != nil {
				log.Printf("Failed to decode batch: %s", err)
				d.Nack(false, false)
				continue
			}

			totalProcessed += len(batch)

			if totalProcessed%100000 == 0 {
				log.Printf("Successfully processed %d records so far...", totalProcessed)
			}
			d.Ack(false)
		}
	}()
	<-forever
	log.Printf("Processing complete in %s", time.Since(startTime))
}
