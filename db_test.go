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
	if count != 1 {
		t.Fatalf("expected 1 user to be in database, but got %d", count)
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
	if count != 0 {
		t.Fatalf("expected 0 user to be in database, but got %d", count)
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
	if count != 4 {
		t.Fatalf("expected 4 users to be in database, but got %d", count)
	}
}

func TestShouldHandlePrepare(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("pgtxdb", "five")
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
	_, err = stmt2.Exec("jane", "jane@gmail.com")
	if err != nil {
		t.Fatalf("should have inserted user - %s", err)
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

func sequentialRollbackTest(t *testing.T, db *sql.DB) error {
	tx1, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx1.Rollback()
	_, err = tx1.Exec(`INSERT INTO app_user(username, email) VALUES ('taro', 'taro@gmail.com')`)
	if err != nil {
		t.Logf("failed to insert the first taro record: %s", err)
		return err
	}
	tx1.Commit()

	tx2, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx2.Rollback()
	_, err = tx2.Exec(`INSERT INTO app_user(username, email) VALUES ('taro', 'taro@gmail.com')`)
	if err != nil {
		t.Logf("successfully failed to insert the second taro record: %s", err)
		return err
	}
	tx2.Commit()
	return nil
}

func TestSavepointRollbackSequential(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("pgtxdb", "six")
	if err != nil {
		t.Fatalf("failed to open a postgres connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	// rollbackTest has to return error since it trys to insert a duplicate record.
	// although it returns error, inside it's function the first record is commited.
	if err := sequentialRollbackTest(t, db); err == nil {
		t.Fatal(err)
	}
	// Thus, we can retreive a record from db scope
	var count int
	err = db.QueryRow(`SELECT count(*) FROM app_user WHERE username = 'taro'`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 user with username taro, but got %d", count)
	}
}

func nestedRollbackTest(t *testing.T, db *sql.DB) error {
	tx1, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx1.Rollback()
	t.Log("tx1 started")
	_, err = tx1.Exec(`INSERT INTO app_user(username, email) VALUES ('taro', 'taro@gmail.com')`)
	if err != nil {
		t.Logf("failed to insert the first taro record: %s", err)
		return err
	}
	tx1.Commit()
	t.Log("tx1 commited")

	tx2, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx2.Rollback()
	t.Log("tx2 started")

	_, err = tx2.Exec(`INSERT INTO app_user(username, email) VALUES ('taro', 'taro@gmail.com')`)
	if err != nil {
		if eventErr := createErrorEventWithTx(t, tx2, db); eventErr != nil {
			return fmt.Errorf("createErrorEvent failed %s", eventErr)
		}
		return err
	}
	tx2.Commit()
	return nil
}

func createErrorEventWithTx(t *testing.T, prevTx *sql.Tx, db *sql.DB) error {
	// need to rollback error tx before starting new tx
	prevTx.Rollback()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	t.Log("error event tx started")
	_, err = tx.Exec(`INSERT INTO error_event (message) values ('error creating app_user')`)
	if err != nil {
		return err
	}
	tx.Commit()
	t.Log("error event tx commited")
	return nil
}

func TestSavepointRollbackNested(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("pgtxdb", "seven")
	if err != nil {
		t.Fatalf("failed to open a postgres connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	if err := nestedRollbackTest(t, db); err == nil {
		t.Fatal(err)
	}

	var count int
	err = db.QueryRow(`SELECT count(*) FROM app_user WHERE username = 'taro'`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 user with username taro, but got %d", count)
	}
	var errCount int
	err = db.QueryRow(`SELECT count(*) FROM error_event`).Scan(&errCount)
	if err != nil {
		t.Fatal(err)
	}
	if errCount != 1 {
		t.Errorf("expected 1 error event, but got %d", errCount)
	}
}
