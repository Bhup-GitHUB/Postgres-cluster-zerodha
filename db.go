package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"time"

	_ "github.com/lib/pq"
)

const defaultDatabaseURL = "postgres://demo:demo@localhost:55432/reports?sslmode=disable"

func openDB() (*sql.DB, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultDatabaseURL
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func initSchema(ctx context.Context, db *sql.DB) error {
	queries := []string{
		`
		CREATE TABLE IF NOT EXISTS orders (
			id BIGSERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL,
			symbol TEXT NOT NULL,
			side TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			price NUMERIC(12, 2) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
		`,
		`
		CREATE TABLE IF NOT EXISTS report_jobs (
			id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			result_table TEXT,
			error TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			completed_at TIMESTAMPTZ
		)
		`,
	}

	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return err
		}
	}

	return nil
}

func seedOrders(ctx context.Context, db *sql.DB) error {
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM orders`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	symbols := []string{"NIFTY", "BANKNIFTY", "INFY", "TCS", "RELIANCE", "HDFCBANK", "SBIN"}
	sides := []string{"BUY", "SELL"}
	rng := rand.New(rand.NewSource(42))

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO orders (user_id, symbol, side, quantity, price, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < 5000; i++ {
		userID := rng.Intn(900) + 100
		symbol := symbols[rng.Intn(len(symbols))]
		side := sides[rng.Intn(len(sides))]
		quantity := (rng.Intn(20) + 1) * 25
		price := 100 + rng.Float64()*2500
		createdAt := time.Now().Add(-time.Duration(rng.Intn(30*24)) * time.Hour)

		if _, err := stmt.ExecContext(ctx, userID, symbol, side, quantity, fmt.Sprintf("%.2f", price), createdAt); err != nil {
			return err
		}
	}

	return tx.Commit()
}
