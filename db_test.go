package pgtxdb

import (
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"testing"

	_ "github.com/lib/pq" // postgres
)

func init() {
	// we register an sql driver txdb
	Register("pgtxdb", "postgres", "user=pgtest dbname=pgtest sslmode=disable")
}

func TestShouldRunWithinTransaction(t *testing.T) {
	t.Parallel()
	var count int
	db1, err := sql.Open("pgtxdb", "one")
	if err != nil {
		t.Fatalf("failed to open a postgres connection, have you run 'make test'? err: %s", err)
	}
	defer db1.Close()

	_, err = db1.Exec(`INSERT INTO app_user(username, email) VALUES('txdb', 'txdb@test.com')`)
	if err != nil {
		t.Fatalf("failed to insert an app_user: %s", err)
	}
	err = db1.QueryRow("SELECT COUNT(id) FROM app_user").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count users: %s", err)
	}
	if count != 4 {
		t.Fatalf("expected 4 user to be in database, but got %d", count)
	}

	db2, err := sql.Open("pgtxdb", "two")
	if err != nil {
		t.Fatalf("failed to reopen a postgres connection: %s", err)
	}
	defer db2.Close()

	err = db2.QueryRow("SELECT COUNT(id) FROM app_user").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count app_user: %s", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 user to be in database, but got %d", count)
	}
}

func TestShouldNotHoldConnectionForRows(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("pgtxdb", "three")
	if err != nil {
		t.Fatalf("failed to open a postgres connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT username FROM app_user")
	if err != nil {
		t.Fatalf("failed to query users: %s", err)
	}
	defer rows.Close()

	_, err = db.Exec(`INSERT INTO app_user(username, email) VALUES('txdb', 'txdb@test.com')`)
	if err != nil {
		t.Fatalf("failed to insert an app_user: %s", err)
	}
}

func TestShouldPerformParallelActions(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	t.Parallel()
	db, err := sql.Open("pgtxdb", "four")
	if err != nil {
		t.Fatalf("failed to open a postgres connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	wg := &sync.WaitGroup{}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(d *sql.DB, idx int) {
			defer wg.Done()
			rows, err := d.Query("SELECT username FROM app_user")
			if err != nil {
				t.Fatalf("failed to query app_user: %s", err)
			}
			defer rows.Close()

			username := fmt.Sprintf("parallel%d", idx)
			email := fmt.Sprintf("parallel%d@test.com", idx)
			_, err = d.Exec(`INSERT INTO app_user(username, email) VALUES($1, $2)`, username, email)
			if err != nil {
				t.Fatalf("failed to insert an app_user: %s", err)
			}
		}(db, i)
	}
	wg.Wait()
	var count int
	err = db.QueryRow("SELECT COUNT(id) FROM app_user").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count users: %s", err)
	}
	if count != 7 {
		t.Fatalf("expected 7 users to be in database, but got %d", count)
	}
}

func TestShouldHandlePrepare(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("pgtxdb", "three")
	if err != nil {
		t.Fatalf("failed to open a postgres connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	stmt1, err := db.Prepare("SELECT email FROM app_user WHERE username = $1")
	if err != nil {
		t.Fatalf("could not prepare - %s", err)
	}

	stmt2, err := db.Prepare("INSERT INTO app_user(username, email) VALUES($1, $2)")
	if err != nil {
		t.Fatalf("could not prepare - %s", err)
	}

	var email string
	if err = stmt1.QueryRow("jane").Scan(&email); err != nil {
		t.Fatalf("could not scan email - %s", err)
	}

	_, err = stmt2.Exec("mark", "mark.spencer@gmail.com")
	if err != nil {
		t.Fatalf("should have inserted user - %s", err)
	}
}
