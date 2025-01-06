package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	totalRows  = 17611148  // Total rows to generate
	batchSize  = 1000      // Number of rows per batch
	numWorkers = 12        // Number of parallel workers
	totalUsers = totalRows // int(averageReplies)
)

var (
	states       = []string{"ACTIVE", "BLOCKED", "DELETED"}
	stateWeights = []int{80, 10, 10} // Distribution percentages
)

type Reply struct {
	Reply         string
	ReferenceType string
	ReferenceID   string
	UserID        int64
	State         string
	DateCreated   time.Time
	DateUpdated   time.Time
}

func worker(db *sql.DB, replies <-chan []Reply, wg *sync.WaitGroup, progress *int64, mutex *sync.Mutex) {
	defer wg.Done()

	for batch := range replies {
		err := insertReplies(db, batch)
		if err != nil {
			log.Printf("failed to insert batch: %v", err)
		} else {
			mutex.Lock()
			*progress += int64(len(batch))
			log.Printf("Progress: %d/%d rows inserted", *progress, totalRows)
			mutex.Unlock()
		}
	}
}

func insertReplies(db *sql.DB, replies []Reply) error {
	query := "INSERT INTO reply (reply, reference_type, reference_id, user_id, state, date_created, date_updated) VALUES "
	values := make([]interface{}, 0, len(replies)*7)

	for _, reply := range replies {
		query += "(?, ?, ?, ?, ?, ?, ?),"
		values = append(values, reply.Reply, reply.ReferenceType, reply.ReferenceID, reply.UserID, reply.State, reply.DateCreated, reply.DateUpdated)
	}

	query = query[:len(query)-1] // Remove trailing comma

	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(values...)
	if err != nil {
		return fmt.Errorf("failed to execute statement: %w", err)
	}

	return nil
}

func main() {
	dsn := "root:123456@tcp(127.0.0.1:3306)/social"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to connect to DB: %v", err)
	}
	defer db.Close()

	var progress int64 = 0
	var mutex sync.Mutex

	wg := sync.WaitGroup{}
	replyChannel := make(chan []Reply, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(db, replyChannel, &wg, &progress, &mutex)
	}

	// generate and send data
	for i := 0; i < totalRows/batchSize; i++ {
		replies := generateReplies(batchSize)
		replyChannel <- replies
	}

	close(replyChannel)
	wg.Wait()

	fmt.Println("Data generated and inserted")
}

func generateReplies(count int) []Reply {
	replies := make([]Reply, count)
	for i := 0; i < count; i++ {
		replies[i] = Reply{
			Reply:         generateRandomText(),
			ReferenceType: "COMMENT",
			ReferenceID:   fmt.Sprintf("ref_%d", rand.Int63n(10000000)),
			UserID:        rand.Int63n(totalUsers) + 1,
			State:         weightedRandomChoice(states, stateWeights),
			DateCreated:   randomTimestamp(),
			DateUpdated:   randomTimestamp(),
		}
	}
	return replies
}

func generateRandomText() string {
	return fmt.Sprintf("this is a random reply %d", rand.Int63n(10000000))
}

func weightedRandomChoice(choices []string, weights []int) string {
	totalWeight := 0
	for _, weight := range weights {
		totalWeight += weight
	}
	r := rand.Intn(totalWeight)
	for i, weight := range weights {
		if r < weight {
			return choices[i]
		}
		r -= weight
	}
	return choices[len(choices)-1]
}

func randomTimestamp() time.Time {
	return time.Now().Add(-time.Duration(rand.Intn(365*24)) * time.Hour)
}
