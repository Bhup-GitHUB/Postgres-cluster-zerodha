package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var tableNamePattern = regexp.MustCompile(`^report_result_[a-f0-9]+$`)

type app struct {
	db *sql.DB
}

type reportJob struct {
	ID          string `json:"job_id"`
	Status      string `json:"status"`
	ResultTable sql.NullString
	Error       sql.NullString
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type reportRow struct {
	Symbol      string  `json:"symbol"`
	Side        string  `json:"side"`
	Orders      int     `json:"orders"`
	Quantity    int     `json:"quantity"`
	AverageRate float64 `json:"average_rate"`
	Turnover    float64 `json:"turnover"`
}

func main() {
	ctx := context.Background()

	db, err := openDB()
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := initSchema(ctx, db); err != nil {
		log.Fatalf("init schema: %v", err)
	}
	if err := seedOrders(ctx, db); err != nil {
		log.Fatalf("seed orders: %v", err)
	}

	a := &app{db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /reports", a.createReport)
	mux.HandleFunc("GET /reports/", a.getReport)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	log.Println("server listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}

func (a *app) createReport(w http.ResponseWriter, r *http.Request) {
	jobID, err := newJobID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create job id")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	_, err = a.db.ExecContext(ctx, `
		INSERT INTO report_jobs (id, status, created_at, updated_at)
		VALUES ($1, 'queued', now(), now())
	`, jobID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not queue report")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"job_id": jobID,
		"status": "queued",
	})
}

func (a *app) getReport(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimPrefix(r.URL.Path, "/reports/")
	if jobID == "" || strings.Contains(jobID, "/") {
		writeError(w, http.StatusBadRequest, "expected /reports/{job_id}")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	job, err := a.getJob(ctx, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "report job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load report job")
		return
	}

	response := map[string]any{
		"job_id": job.ID,
		"status": job.Status,
	}

	if job.Error.Valid {
		response["error"] = job.Error.String
	}

	if job.Status != "done" {
		writeJSON(w, http.StatusOK, response)
		return
	}

	rows, err := a.loadReportRows(ctx, job.ResultTable.String)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load cached report")
		return
	}

	response["rows"] = rows
	writeJSON(w, http.StatusOK, response)
}

func (a *app) getJob(ctx context.Context, jobID string) (reportJob, error) {
	var job reportJob
	err := a.db.QueryRowContext(ctx, `
		SELECT id, status, result_table, error, created_at, updated_at, completed_at
		FROM report_jobs
		WHERE id = $1
	`, jobID).Scan(&job.ID, &job.Status, &job.ResultTable, &job.Error, &job.CreatedAt, &job.UpdatedAt, &job.CompletedAt)
	return job, err
}

func (a *app) loadReportRows(ctx context.Context, tableName string) ([]reportRow, error) {
	if !tableNamePattern.MatchString(tableName) {
		return nil, fmt.Errorf("unsafe result table name %q", tableName)
	}

	rows, err := a.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT symbol, side, orders, quantity, average_rate, turnover
		FROM %s
		ORDER BY turnover DESC
		LIMIT 50
	`, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []reportRow
	for rows.Next() {
		var row reportRow
		if err := rows.Scan(&row.Symbol, &row.Side, &row.Orders, &row.Quantity, &row.AverageRate, &row.Turnover); err != nil {
			return nil, err
		}
		result = append(result, row)
	}

	return result, rows.Err()
}

func newJobID() (string, error) {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write json: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
