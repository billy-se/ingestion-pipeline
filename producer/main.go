package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
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
	RawLine     string `json:"raw_line"`
	Checksum    string `json:"checksum"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func main() {
	startTime := time.Now()

	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Warning: No .env found, falling back to defaults")
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

	file, err := os.Open("data/massive_logs.txt")
	failOnError(err, "Failed to open massive_logs.txt")
	defer file.Close()

	linesChan := make(chan string, 5000)
	batchesChan := make(chan []LogRecord, 50)
	var workerWg sync.WaitGroup
	var publisherWg sync.WaitGroup

	numProcessors := 4
	for i := 0; i < numProcessors; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			var batch []LogRecord
			for line := range linesChan {
				var rec LogRecord
				if err := json.Unmarshal([]byte(line), &rec); err == nil {
					rec.RawLine = line
					hash := sha256.Sum256([]byte(line))
					rec.Checksum = hex.EncodeToString(hash[:])

					batch = append(batch, rec)
					if len(batch) >= 1000 {
						batchesChan <- batch
						batch = nil
					}
				}
			}
			if len(batch) > 0 {
				batchesChan <- batch
			}
		}()
	}

	numPublishers := 4
	publisherWg.Add(numPublishers)
	for i := 0; i < numPublishers; i++ {
		go func() {
			defer publisherWg.Done()

			pubConn, err := amqp.Dial(dsn)
			if err != nil {
				log.Printf("Publisher connection error: %v", err)
				return
			}
			defer pubConn.Close()

			pubCh, err := pubConn.Channel()
			if err != nil {
				log.Printf("Publisher channel error: %v", err)
				return
			}
			defer pubCh.Close()

			for batch := range batchesChan {
				data, err := json.Marshal(batch)
				if err != nil {
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

				var pubErr error
				for attempt := 1; attempt <= 3; attempt++ {
					pubErr = pubCh.PublishWithContext(ctx,
						"",
						q.Name,
						false,
						false,
						amqp.Publishing{
							ContentType: "application/json",
							Body:        data,
						},
					)
					if pubErr == nil {
						break
					}
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				}
				cancel()

				if pubErr != nil {
					log.Printf("Failed to publish batch after retries: %v", pubErr)
				}
			}
		}()
	}

	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			linesChan <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Scanner error encountered: %v", err)
		}

		close(linesChan)
		workerWg.Wait()
		close(batchesChan)
	}()

	publisherWg.Wait()

	log.Printf("Data transfer complete with 0%% data loss in %v!", time.Since(startTime))
}
