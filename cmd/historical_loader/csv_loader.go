package main

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type OHLCVTData struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Trades    int
	Interval  int // in minutes
	Symbol    string
}

type ProcessingState struct {
	ProcessedFiles map[string]bool
	LastUpdate     time.Time
}

func main() {
	// Configuration - you might want to move these to environment variables
	dbConnStr := "postgres://postgres:password@localhost/postgres"
	zipFilePath := "C:\\Users\\sajis\\Downloads\\Kraken_OHLCVT.zip"

	// Connect to TimescaleDB with optimized pool settings
	config, err := pgxpool.ParseConfig(dbConnStr)
	if err != nil {
		log.Fatalf("Unable to parse database config: %v", err)
	}

	// Optimize connection pool settings
	config.MaxConns = 8
	config.MinConns = 4
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	ctx := context.Background()
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer pool.Close()

	// Create hypertable if it doesn't exist
	if err := createHypertable(ctx, pool); err != nil {
		log.Fatalf("Failed to create hypertable: %v", err)
	}

	// Process the ZIP file
	if err := processZipFile(ctx, pool, zipFilePath); err != nil {
		log.Fatalf("Failed to process ZIP file: %v", err)
	}
}

func createHypertable(ctx context.Context, pool *pgxpool.Pool) error {
	queries := []string{
		// Disable indexing during bulk load
		`DROP INDEX IF EXISTS idx_ohlcvt_symbol`,
		`DROP INDEX IF EXISTS idx_ohlcvt_interval`,

		`CREATE TABLE IF NOT EXISTS ohlcvt_data (
			timestamp TIMESTAMPTZ NOT NULL,
			symbol TEXT NOT NULL,
			interval_minutes INTEGER NOT NULL,
			open DOUBLE PRECISION NOT NULL,
			high DOUBLE PRECISION NOT NULL,
			low DOUBLE PRECISION NOT NULL,
			close DOUBLE PRECISION NOT NULL,
			volume DOUBLE PRECISION NOT NULL,
			trades INTEGER NOT NULL
		)`,
		`SELECT create_hypertable('ohlcvt_data', 'timestamp', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 week')`,

		// Set optimal TimescaleDB parameters for bulk loading
		// `ALTER DATABASE postgres SET timescaledb.max_background_workers = 8`,
		// `ALTER DATABASE postgres SET maintenance_work_mem = '1GB'`,
		// `ALTER DATABASE postgres SET max_parallel_workers_per_gather = 4`,
		// `ALTER DATABASE postgres SET max_parallel_workers = 8`,
		// `ALTER DATABASE postgres SET work_mem = '128MB'`,
	}

	for _, query := range queries {
		_, err := pool.Exec(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to execute query '%s': %v", query, err)
		}
	}
	return nil
}

func processZipFile(ctx context.Context, pool *pgxpool.Pool, zipFilePath string) error {
	reader, err := zip.OpenReader(zipFilePath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %v", err)
	}
	defer reader.Close()

	log.Printf("Processing ZIP file: %s", zipFilePath)

	// Load processing state
	state, err := loadProcessingState()
	if err != nil {
		log.Printf("Warning: Could not load processing state: %v", err)
		state = &ProcessingState{
			ProcessedFiles: make(map[string]bool),
			LastUpdate:     time.Now(),
		}
	}

	// Process files concurrently with worker pool
	workerCount := 4
	filesChan := make(chan *zip.File, len(reader.File))
	errorsChan := make(chan error, len(reader.File))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range filesChan {
				// Skip if already processed
				if state.ProcessedFiles[file.Name] {
					log.Printf("Skipping already processed file: %s", file.Name)
					continue
				}

				if !strings.HasSuffix(strings.ToLower(file.Name), ".csv") {
					continue
				}

				interval, symbol := parseFileInfo(file.Name)
				if interval == 0 {
					continue
				}

				log.Printf("Processing %s (Symbol: %s, Interval: %d minutes)", file.Name, symbol, interval)
				if err := processCSVFile(ctx, pool, file, interval, symbol); err != nil {
					log.Printf("Error processing %s: %v", file.Name, err)
					errorsChan <- err
					continue // Continue with next file instead of returning
				}

				// Mark file as processed and save state
				state.ProcessedFiles[file.Name] = true
				state.LastUpdate = time.Now()
				if err := saveProcessingState(state); err != nil {
					log.Printf("Warning: Failed to save processing state: %v", err)
				}
			}
		}()
	}

	// Send files to workers
	for _, file := range reader.File {
		filesChan <- file
	}
	close(filesChan)

	// Wait for workers to finish
	wg.Wait()
	close(errorsChan)

	// Log all errors but don't fail
	var errors []error
	for err := range errorsChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		log.Printf("Completed with %d errors:", len(errors))
		for _, err := range errors {
			log.Printf("- %v", err)
		}
	}

	// Recreate indexes after bulk load
	queries := []string{
		`CREATE INDEX IF NOT EXISTS idx_ohlcvt_symbol ON ohlcvt_data(symbol)`,
		`CREATE INDEX IF NOT EXISTS idx_ohlcvt_interval ON ohlcvt_data(interval_minutes)`,
	}

	for _, query := range queries {
		_, err := pool.Exec(ctx, query)
		if err != nil {
			log.Printf("Warning: Failed to create index: %v", err)
		}
	}

	return nil
}

func parseFileInfo(filename string) (interval int, symbol string) {
	// Only process 1-minute interval files (ending with _1.csv)
	if !strings.HasSuffix(filename, "_1.csv") {
		log.Printf("Skipping non 1-minute interval file: %s", filename)
		return 0, ""
	}

	// Remove the "_1.csv" suffix and use the remainder as the symbol
	symbol = strings.TrimSuffix(filename, "_1.csv")

	// Return 1 as the interval (1 minute)
	return 1, symbol
}

func parseRecord(record []string, interval int, symbol string) (OHLCVTData, error) {
	// Implement based on your CSV format
	// Example implementation:
	timestamp, err := strconv.ParseInt(record[0], 10, 64)
	if err != nil {
		return OHLCVTData{}, err
	}

	open, _ := strconv.ParseFloat(record[1], 64)
	high, _ := strconv.ParseFloat(record[2], 64)
	low, _ := strconv.ParseFloat(record[3], 64)
	close, _ := strconv.ParseFloat(record[4], 64)
	volume, _ := strconv.ParseFloat(record[5], 64)
	trades, _ := strconv.Atoi(record[6])

	return OHLCVTData{
		Timestamp: time.Unix(timestamp, 0),
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close,
		Volume:    volume,
		Trades:    trades,
		Interval:  interval,
		Symbol:    symbol,
	}, nil
}

func processCSVFile(ctx context.Context, pool *pgxpool.Pool, file *zip.File, interval int, symbol string) error {
	fileReader, err := file.Open()
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)
	rowCount := 0

	// Increase batch size for better performance
	const batchSize = 5000
	rows := make([][]interface{}, 0, batchSize)

	// Skip header if exists
	_, err = csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("empty file")
		}
		return fmt.Errorf("failed to read header: %v", err)
	}

	// Start a transaction
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Read and process records
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record: %v", err)
		}

		data, err := parseRecord(record, interval, symbol)
		if err != nil {
			log.Printf("Skipping invalid record in %s: %v", file.Name, err)
			continue
		}

		rows = append(rows, []interface{}{
			data.Timestamp, data.Symbol, data.Interval,
			data.Open, data.High, data.Low, data.Close,
			data.Volume, data.Trades,
		})
		rowCount++

		if len(rows) >= batchSize {
			if err := copyRows(ctx, tx, rows); err != nil {
				return fmt.Errorf("failed to copy batch: %v", err)
			}
			log.Printf("Inserted %d rows for %s", len(rows), file.Name)
			rows = rows[:0]
		}
	}

	// Insert remaining rows
	if len(rows) > 0 {
		if err := copyRows(ctx, tx, rows); err != nil {
			return fmt.Errorf("failed to copy final batch: %v", err)
		}
		log.Printf("Inserted final %d rows for %s", len(rows), file.Name)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log.Printf("Completed processing %s: %d total rows inserted", file.Name, rowCount)
	return nil
}

func copyRows(ctx context.Context, tx pgx.Tx, rows [][]interface{}) error {
	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"ohlcvt_data"},
		[]string{"timestamp", "symbol", "interval_minutes", "open", "high", "low", "close", "volume", "trades"},
		pgx.CopyFromRows(rows),
	)
	return err
}

func loadProcessingState() (*ProcessingState, error) {
	data, err := os.ReadFile("processing_state.json")
	if err != nil {
		if os.IsNotExist(err) {
			return &ProcessingState{
				ProcessedFiles: make(map[string]bool),
				LastUpdate:     time.Now(),
			}, nil
		}
		return nil, err
	}

	var state ProcessingState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func saveProcessingState(state *ProcessingState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("processing_state.json", data, 0644)
}
