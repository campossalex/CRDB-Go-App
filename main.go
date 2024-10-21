package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
)

var version string

func main() {
	log.SetFlags(0)

	url := flag.String("url", "", "database connection string")
	showVersion := flag.Bool("version", false, "show the application version number")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	r := runner{
		accounts: 1000,
	}

	db, err := pgxpool.New(context.Background(), *url)
	if err != nil {
		log.Fatalf("error connecting to database: %v", err)
	}
	defer db.Close()

	if err = db.Ping(context.Background()); err != nil {
		log.Fatalf("error testing database connection: %v", err)
	}

	if err := r.deinit(db); err != nil {
		log.Fatalf("error destroying database: %v", err)
	}

	if err := r.init(db); err != nil {
		log.Fatalf("error initialising database: %v", err)
	}

	if err := r.run(db); err != nil {
		log.Fatalf("error running simulation: %v", err)
	}
}

type runner struct {
	accounts int

	accountIDs   []string
	selectionIDs []string
}

func (r *runner) init(db *pgxpool.Pool) error {
	if err := create(db); err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	if err := seed(db, r.accounts); err != nil {
		return fmt.Errorf("seeding database: %w", err)
	}

	accountIDs, err := fetchIDs(db, r.accounts)
	if err != nil {
		return fmt.Errorf("fetching ids: %w", err)
	}

	r.accountIDs = accountIDs
	return nil
}

func (r *runner) deinit(db *pgxpool.Pool) error {
	if err := drop(db); err != nil {
		log.Fatalf("error creating database: %v", err)
	}

	return nil
}

func (r *runner) run(db *pgxpool.Pool) error {
	accountIDs, err := fetchIDs(db, r.accounts)
	if err != nil {
		log.Fatalf("error fetching ids ahead of test: %v", err)
	}
	r.selectionIDs = accountIDs

	// Work loop.
	for range time.NewTicker(time.Millisecond * 100).C {
		ids := lo.Samples(r.selectionIDs, 2)

		taken, err := performTransfer(db, ids[0], ids[1], rand.Intn(100))
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("error: %v", err)
		}

		log.Printf(
			"%s -> %s took %dms",
			ids[0][0:8],
			ids[1][0:8],
			taken.Milliseconds(),
		)
	}

	panic("unexected app termination")
}

func performTransfer(db *pgxpool.Pool, from, to string, amount int) (elapsed time.Duration, err error) {
	timeout, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	defer func() {
		elapsed = time.Since(start)
	}()

	const stmt = `UPDATE account
									SET balance = CASE 
											WHEN id = $1 THEN balance - $3
											WHEN id = $2 THEN balance + $3
									END
								WHERE id IN ($1, $2);`

	err = crdbpgx.ExecuteTx(timeout, db, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err = db.Exec(timeout, stmt, from, to, amount)
		return err
	})

	return
}

// create the database infrastructure required by the load test.
func create(db *pgxpool.Pool) error {
	const stmt = `CREATE TABLE account (
									id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
									balance DECIMAL NOT NULL
								)`

	_, err := db.Exec(context.Background(), stmt)
	return err
}

// seed the database with data.
func seed(db *pgxpool.Pool, rowCount int) error {
	const stmt = `INSERT INTO account (balance)
								SELECT 10000
								FROM generate_series(1, $1)`

	_, err := db.Exec(context.Background(), stmt, rowCount)
	return err
}

// drop the database infrastructure required by the load test.
func drop(db *pgxpool.Pool) error {
	const stmt = `DROP TABLE IF EXISTS account`

	_, err := db.Exec(context.Background(), stmt)
	return err
}

// fetchIDs returns a selection of ids from the account table.
func fetchIDs(db *pgxpool.Pool, count int) ([]string, error) {
	const stmt = `SELECT id FROM account ORDER BY random() LIMIT $1`

	rows, err := db.Query(context.Background(), stmt, count)
	if err != nil {
		return nil, fmt.Errorf("querying for rows: %w", err)
	}

	var accountIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning id: %w", err)
		}
		accountIDs = append(accountIDs, id)
	}

	return accountIDs, nil
}
