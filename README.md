# Zerodha Postgres Cache Demo

A tiny Go + PostgreSQL demo inspired by Zerodha's DungBeetle reporting pattern.

The idea is simple: do not keep hammering the main database every time a user
sorts, filters, or reloads a heavy report. Run the expensive report query once,
copy the result into a temporary Postgres table, and serve the user from that
cached table.

## Architecture

```text
User requests report
        |
        v
Go API creates async job
        |
        v
Worker picks queued job
        |
        v
Worker runs heavy SQL on source table
        |
        v
Creates report_result_<job_id>
        |
        v
User reads cached report rows
        |
        v
Cleanup drops old result tables
```

This demo keeps everything in one service so the pattern is easy to understand.
The `orders` table acts like the source database. The `report_result_<job_id>`
tables act like Zerodha's hot Postgres cache tables.

## Stack

- Go
- PostgreSQL
- Docker Compose

## Run

Start Postgres:

```bash
docker compose up -d
```

Run the API:

```bash
go run .
```

The server listens on `http://localhost:8080`.

## API

Create a report job:

```bash
curl -X POST http://localhost:8080/reports
```

Example response:

```json
{
  "job_id": "abc123",
  "status": "queued"
}
```

Fetch the report:

```bash
curl http://localhost:8080/reports/abc123
```

If the worker is still running:

```json
{
  "job_id": "abc123",
  "status": "running"
}
```

When complete:

```json
{
  "job_id": "abc123",
  "rows": [
    {
      "symbol": "NIFTY",
      "side": "BUY",
      "orders": 381,
      "quantity": 198750,
      "average_rate": 1412.33,
      "turnover": 280670587.5
    }
  ],
  "status": "done"
}
```

## What This Demonstrates

- `POST /reports` creates a queued async job.
- A Go worker polls `report_jobs`.
- The worker creates one cached result table per report job.
- `GET /reports/{job_id}` reads from the cached result table after completion.
- A cleanup loop drops old `report_result_*` tables after a short retention
  window.

In a real setup, the cleanup could be a nightly wipe of the cache database. That
is the key idea from the Zerodha story: protect the main database by moving
repeated user-facing reads to disposable Postgres result tables.
