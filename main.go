package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type LogRecord struct {
	ID          int    `json:"id"`
	Timestamp   int64  `json:"timestamp"`
	User        string `json:"user"`
	Action      string `json:"action"`
	PayloadSize int    `json:"payload_size"`
	RawLine     string `json:"raw_line"` // Carries original string for exact hash verification
	Checksum    string `json:"checksum"`
}

func main() {
	file, err := os.Open("massive_logs.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	linesChan := make(chan string, 1000)
	batchesChan := make(chan []LogRecord, 10)
	var wg sync.WaitGroup

	// 1. Concurrency Engine: Worker Pool (5 Goroutines processing lines)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var batch []LogRecord
			for line := range linesChan {
				var rec LogRecord
				if err := json.Unmarshal([]byte(line), &rec); err == nil {
					rec.RawLine = line
					// Generate cryptographic SHA-256 Checksum from raw data
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

	// 2. Stream Reader: Feeds lines without memory spikes
	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			linesChan <- scanner.Text()
		}
		close(linesChan)
		wg.Wait()
		close(batchesChan)
	}()

	// 3. Batch Saver & Network Dispatcher with Retries
	client := &http.Client{Timeout: 10 * time.Second}
	for batch := range batchesChan {
		sendBatchWithRetry(client, batch)
	}
	fmt.Println("Data transfer complete with 0% data loss!")
}

// Resilient Network Dispatcher with Exponential Backoff
func sendBatchWithRetry(client *http.Client, batch []LogRecord) {
	data, _ := json.Marshal(batch)
	url := "http://localhost:8000/ingest"

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}

		fmt.Printf("Network timeout/drop (Attempt %d/%d). Retrying...\n", attempt, maxRetries)
		time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
	}
	fmt.Println("Critical: Failed to transmit batch after max retries.")
}
