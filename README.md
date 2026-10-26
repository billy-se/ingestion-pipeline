# ingestion-pipeline

## What it is & Why it Exists
`ingestion-pipeline` is a high-performance data processing system built with Go and RabbitMQ. It was designed to solve the challenge of reliably handling massive volumes of log data (10+ million records) without crashing your system or running out of memory. 

Instead of overwhelming your infrastructure all at once, this pipeline uses a decoupled producer-consumer architecture:
- **Low Memory Streaming:** Reads huge files line-by-line using Go's `bufio.Scanner` to keep memory usage flat.
- **Concurrent Workers:** Utilizes parallel worker pools to parse, hash, and publish data at lightning speed.
- **Data Integrity:** Applies live SHA-256 cryptographic hashing to verify every single record.
- **Reliability:** Uses RabbitMQ with QoS throttling and manual acknowledgements to guarantee zero data loss, even during high-load traffic spikes.

---

## How to Run

### 1. Prerequisites
- Go (v1.20 or higher)
- RabbitMQ running locally on port `5672`
- A sample log file placed at `data/massive_logs.txt`

### 2. Start the Consumer
Open a terminal and start the consumer worker so it is listening for incoming data:
```bash
go run consumer/main.go
```
then in another terminal. run:
```bash
go run producer/main.go
```