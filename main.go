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
	totalRows       = 98325709  // Total rows to generate
	batchSize       = 1000      // Number of rows per batch
	numWorkers      = 4         // Number of parallel workers
	averageComments = 9.52      // Average comments per user
	maxComments     = 1520      // Max comments per user
	totalUsers      = totalRows // int(averageComments)
)

var (
	states         = []string{"ACTIVE", "BLOCKED", "DELETED", "INVALID"}
	stateWeights   = []int{95, 1, 3, 1} // Distribution percentages
	refTypes       = []string{"REVIEW", "PARCHA"}
	refTypeWeights = []int{99, 1} // Distribution percentages
)

type Comment struct {
	Comment       string
	ReferenceType string
	ReferenceID   string
	UserID        int64
	State         string
	DateCreated   time.Time
	DateUpdated   time.Time
	OldCommentID  int64
}

func worker(db *sql.DB, comments <-chan []Comment, wg *sync.WaitGroup, progress *int64, mutex *sync.Mutex) {
	defer wg.Done()

	for batch := range comments {
		err := insertComments(db, batch)
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

func insertComments(db *sql.DB, comments []Comment) error {
	query := "INSERT INTO comment (comment, reference_type, reference_id, user_id, state, date_created, date_updated, old_comment_id) VALUES "
	values := make([]interface{}, 0, len(comments)*8)

	for _, comment := range comments {
		query += "(?, ?, ?, ?, ?, ?, ?, ?),"
		values = append(values, comment.Comment, comment.ReferenceType, comment.ReferenceID, comment.UserID, comment.State, comment.DateCreated, comment.DateUpdated, comment.OldCommentID)
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
	commentChannel := make(chan []Comment, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(db, commentChannel, &wg, &progress, &mutex)
	}

	// generate and send data
	for i := 0; i < totalRows/batchSize; i++ {
		comments := generateComments(batchSize)
		commentChannel <- comments
	}

	close(commentChannel)
	wg.Wait()

	fmt.Println("Data generated and inserted")
}

func generateComments(count int) []Comment {
	comments := make([]Comment, count)
	for i := 0; i < count; i++ {
		comments[i] = Comment{
			Comment:       generateRandomText(),
			ReferenceType: weightedRandomChoice(refTypes, refTypeWeights),
			ReferenceID:   fmt.Sprintf("ref_%d", rand.Int63n(10000000)),
			UserID:        rand.Int63n(totalUsers) + 1,
			State:         weightedRandomChoice(states, stateWeights),
			DateCreated:   randomTimestamp(),
			DateUpdated:   randomTimestamp(),
			OldCommentID:  rand.Int63n(1000000),
		}
	}
	return comments
}

func generateRandomText() string {
	return fmt.Sprintf("this is a random comment %d", rand.Int63n(10000000))
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
