package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

func startReportWorker(ctx context.Context, db *sql.DB) {
	ticker := time.NewTicker(2 * time.Second)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := processNextReportJob(ctx, db); err != nil {
					log.Printf("worker: %v", err)
				}
			}
		}
	}()
}

func startCleanupWorker(ctx context.Context, db *sql.DB) {
	ticker := time.NewTicker(30 * time.Second)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := cleanupOldReportTables(ctx, db); err != nil {
					log.Printf("cleanup: %v", err)
				}
			}
		}
	}()
}

func processNextReportJob(ctx context.Context, db *sql.DB) error {
	jobID, err := claimQueuedJob(ctx, db)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}

	log.Printf("worker: running report job %s", jobID)

	tableName := resultTableName(jobID)
	if err := buildReportResultTable(ctx, db, tableName); err != nil {
		markJobFailed(ctx, db, jobID, err)
		return fmt.Errorf("job %s failed: %w", jobID, err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE report_jobs
		SET status = 'done', result_table = $2, updated_at = now(), completed_at = now()
		WHERE id = $1
	`, jobID, tableName); err != nil {
		return err
	}

	log.Printf("worker: completed report job %s into %s", jobID, tableName)
	return nil
}

func claimQueuedJob(ctx context.Context, db *sql.DB) (string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var jobID string
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM report_jobs
		WHERE status = 'queued'
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&jobID)
	if err != nil {
		return "", err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE report_jobs
		SET status = 'running', updated_at = now()
		WHERE id = $1
	`, jobID); err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}

	return jobID, nil
}

func buildReportResultTable(ctx context.Context, db *sql.DB, tableName string) error {
	if !tableNamePattern.MatchString(tableName) {
		return fmt.Errorf("unsafe result table name %q", tableName)
	}

	time.Sleep(2 * time.Second)

	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s AS
		SELECT
			symbol,
			side,
			COUNT(*)::int AS orders,
			SUM(quantity)::int AS quantity,
			ROUND((SUM(price * quantity) / NULLIF(SUM(quantity), 0))::numeric, 2)::float8 AS average_rate,
			ROUND(SUM(price * quantity)::numeric, 2)::float8 AS turnover
		FROM orders
		GROUP BY symbol, side
		ORDER BY turnover DESC
	`, tableName))
	return err
}

func markJobFailed(ctx context.Context, db *sql.DB, jobID string, cause error) {
	_, err := db.ExecContext(ctx, `
		UPDATE report_jobs
		SET status = 'failed', error = $2, updated_at = now(), completed_at = now()
		WHERE id = $1
	`, jobID, cause.Error())
	if err != nil {
		log.Printf("worker: could not mark job %s failed: %v", jobID, err)
	}
}

func resultTableName(jobID string) string {
	return "report_result_" + jobID
}

func cleanupOldReportTables(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `
		SELECT id, result_table
		FROM report_jobs
		WHERE status IN ('done', 'failed')
			AND completed_at < now() - interval '2 minutes'
		ORDER BY completed_at
		LIMIT 25
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type staleJob struct {
		id          string
		resultTable sql.NullString
	}

	var jobs []staleJob
	for rows.Next() {
		var job staleJob
		if err := rows.Scan(&job.id, &job.resultTable); err != nil {
			return err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, job := range jobs {
		if job.resultTable.Valid {
			if !tableNamePattern.MatchString(job.resultTable.String) {
				return fmt.Errorf("unsafe result table name %q", job.resultTable.String)
			}

			if _, err := db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, job.resultTable.String)); err != nil {
				return err
			}
		}

		if _, err := db.ExecContext(ctx, `
			UPDATE report_jobs
			SET status = 'cleaned', updated_at = now()
			WHERE id = $1
		`, job.id); err != nil {
			return err
		}

		log.Printf("cleanup: cleaned report job %s", job.id)
	}

	return nil
}
