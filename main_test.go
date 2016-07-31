package pgtxdb

import (
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/lib/pq" // postgres
)

// TestMain service package setup/teardonw
func TestMain(m *testing.M) {
	db, err := sql.Open("postgres", "user=pgtest dbname=pgtest sslmode=disable")
	if err != nil {
		log.Fatalf("failed to connect test db: %s", err.Error())
	}
	_, err = db.Exec(`
	CREATE TABLE app_user (
	  id BIGSERIAL NOT NULL,
	  username TEXT NOT NULL,
	  email TEXT NOT NULL,
	  PRIMARY KEY (id),
	  UNIQUE (email)
	);
	INSERT INTO app_user (username, email) VALUES 
	    ('gopher', 'gopher@go.com'),
	    ('john', 'john@doe.com'),
	    ('jane', 'jane@doe.com')
	;
	`)
	if err != nil {
		log.Fatalf("failed to create test table: %s", err.Error())
	}
	code := m.Run()
	_, err = db.Exec(`
	DROP TABLE IF EXISTS app_user;
	`)
	if err != nil {
		log.Fatalf("failed to create test table: %s", err.Error())
	}
	os.Exit(code)
}
