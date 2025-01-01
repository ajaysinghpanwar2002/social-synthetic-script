package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	totalRows       = 10        // Total rows to generate
	batchSize       = 2         // Number of rows per batch
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

func main() {
	dsn := "root:123456@tcp(127.0.0.1:3306)/social"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to connect to DB: %v", err)
	}
	defer db.Close()
}
