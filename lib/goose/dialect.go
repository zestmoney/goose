package goose

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/mattn/go-sqlite3"
)

// SqlDialect abstracts the details of specific SQL dialects
// for goose's few SQL specific statements
type SqlDialect interface {
	createVersionTableSql() string // sql string to create the goose_db_version table
	insertVersionSql() string      // sql string to insert the initial version table row
	dbVersionQuery(db *sql.DB) (*sql.Rows, error)
	dbCheckSumQuery(db *sql.DB, version int64) (string, error)
}

// drivers that we don't know about can ask for a dialect by name
func dialectByName(d string) SqlDialect {
	switch d {
	case "postgres":
		return &PostgresDialect{}
	case "mysql":
		return &MySqlDialect{}
	case "sqlite3":
		return &Sqlite3Dialect{}
	}

	return nil
}

////////////////////////////
// Postgres
////////////////////////////

type PostgresDialect struct{}

func (pg PostgresDialect) createVersionTableSql() string {
	return `CREATE TABLE goose_db_version (
            	id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                checksum VARCHAR (50) NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`
}

func (pg PostgresDialect) insertVersionSql() string {
	return "INSERT INTO goose_db_version (version_id, is_applied, checksum) VALUES ($1, $2, $3);"
}

func (pg PostgresDialect) dbVersionQuery(db *sql.DB) (*sql.Rows, error) {
	rows, err := db.Query("SELECT version_id, is_applied from goose_db_version ORDER BY id DESC")

	// XXX: check for postgres specific error indicating the table doesn't exist.
	// for now, assume any error is because the table doesn't exist,
	// in which case we'll try to create it.
	if err != nil {
		return nil, ErrTableDoesNotExist
	}

	return rows, err
}
func (pg PostgresDialect) dbCheckSumQuery(db *sql.DB, version int64) (string, error) {
	return getCheckSum(db, version)
}

////////////////////////////
// MySQL
////////////////////////////

type MySqlDialect struct{}

func (m MySqlDialect) createVersionTableSql() string {
	return `CREATE TABLE goose_db_version (
                id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
				checksum VARCHAR (50) NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`
}

func (m MySqlDialect) insertVersionSql() string {
	return "INSERT INTO goose_db_version (version_id, is_applied, checksum) VALUES (?, ?, ?);"
}

func (m MySqlDialect) dbVersionQuery(db *sql.DB) (*sql.Rows, error) {
	rows, err := db.Query("SELECT version_id, is_applied from goose_db_version ORDER BY id DESC")

	// XXX: check for mysql specific error indicating the table doesn't exist.
	// for now, assume any error is because the table doesn't exist,
	// in which case we'll try to create it.
	if err != nil {
		return nil, ErrTableDoesNotExist
	}

	return rows, err
}

func (m MySqlDialect) dbCheckSumQuery(db *sql.DB, version int64) (string, error) {
	return getCheckSum(db, version)
}

func getCheckSum(db *sql.DB, version int64) (string, error) {
	var checksum string
	query := fmt.Sprintf("SELECT checksum from goose_db_version WHERE version_id = %d", version)
	log.Println("query to execute:", query)
	rows, err := db.Query(query)
	log.Println("row retrived from db:", rows)

	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&checksum)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(checksum)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	return checksum, nil
}

////////////////////////////
// sqlite3
////////////////////////////

type Sqlite3Dialect struct{}

func (m Sqlite3Dialect) createVersionTableSql() string {
	return `CREATE TABLE goose_db_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                is_applied INTEGER NOT NULL,
                tstamp TIMESTAMP DEFAULT (datetime('now'))
            );`
}

func (m Sqlite3Dialect) insertVersionSql() string {
	return "INSERT INTO goose_db_version (version_id, is_applied) VALUES (?, ?);"
}

func (m Sqlite3Dialect) dbVersionQuery(db *sql.DB) (*sql.Rows, error) {
	rows, err := db.Query("SELECT version_id, is_applied from goose_db_version ORDER BY id DESC")

	switch err.(type) {
	case sqlite3.Error:
		return nil, ErrTableDoesNotExist
	}
	return rows, err
}

func (m Sqlite3Dialect) dbCheckSumQuery(db *sql.DB, version int64) (string, error) {
	panic("Check sum column is not present in goose_db_version table, Hence can't retrive it")
	return "", nil
}
