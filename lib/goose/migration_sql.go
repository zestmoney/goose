package goose

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const sqlCmdPrefix = "-- +goose "

// Checks the line to see if the line has a statement-ending semicolon
// or if the line contains a double-dash comment.
func endsWithSemicolon(line string) bool {

	prev := ""
	scanner := bufio.NewScanner(strings.NewReader(line))
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		word := scanner.Text()
		if strings.HasPrefix(word, "--") {
			break
		}
		prev = word
	}

	return strings.HasSuffix(prev, ";")
}

// Split the given sql script into individual statements.
//
// The base case is to simply split on semicolons, as these
// naturally terminate a statement.
//
// However, more complex cases like pl/pgsql can have semicolons
// within a statement. For these cases, we provide the explicit annotations
// 'StatementBegin' and 'StatementEnd' to allow the script to
// tell us to ignore semicolons.
func splitSQLStatements(r io.Reader, direction bool) (stmts []string) {

	var buf bytes.Buffer
	scanner := bufio.NewScanner(r)

	// track the count of each section
	// so we can diagnose scripts with no annotations
	upSections := 0
	downSections := 0

	statementEnded := false
	ignoreSemicolons := false
	directionIsActive := false

	for scanner.Scan() {

		line := scanner.Text()

		// handle any goose-specific commands
		if strings.HasPrefix(line, sqlCmdPrefix) {
			cmd := strings.TrimSpace(line[len(sqlCmdPrefix):])
			switch cmd {
			case "Up":
				directionIsActive = (direction == true)
				upSections++
				break

			case "Down":
				directionIsActive = (direction == false)
				downSections++
				break

			case "StatementBegin":
				if directionIsActive {
					ignoreSemicolons = true
				}
				break

			case "StatementEnd":
				if directionIsActive {
					statementEnded = (ignoreSemicolons == true)
					ignoreSemicolons = false
				}
				break
			}
		}

		if !directionIsActive {
			continue
		}

		if _, err := buf.WriteString(line + "\n"); err != nil {
			log.Fatalf("io err: %v", err)
		}

		// Wrap up the two supported cases: 1) basic with semicolon; 2) psql statement
		// Lines that end with semicolon that are in a statement block
		// do not conclude statement.
		if (!ignoreSemicolons && endsWithSemicolon(line)) || statementEnded {
			statementEnded = false
			stmts = append(stmts, buf.String())
			buf.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("scanning migration: %v", err)
	}

	// diagnose likely migration script errors
	if ignoreSemicolons {
		log.Println("WARNING: saw '-- +goose StatementBegin' with no matching '-- +goose StatementEnd'")
	}

	if bufferRemaining := strings.TrimSpace(buf.String()); len(bufferRemaining) > 0 {
		log.Printf("WARNING: Unexpected unfinished SQL query: %s. Missing a semicolon?\n", bufferRemaining)
	}

	if upSections == 0 && downSections == 0 {
		log.Fatalf(`ERROR: no Up/Down annotations found, so no statements were executed.
			See https://bitbucket.org/liamstask/goose/overview for details.`)
	}

	return
}

// Run a migration specified in raw SQL.
//
// Sections of the script can be annotated with a special comment,
// starting with "-- +goose" to specify whether the section should
// be applied during an Up or Down migration
//
// All statements following an Up or Down directive are grouped together
// until another direction directive is found.
func runSQLMigration(conf *DBConf, db *sql.DB, scriptFile string, v int64, direction bool) error {

	txn, err := db.Begin()
	if err != nil {
		log.Fatal("db.Begin:", err)
	}

	f, err := os.Open(scriptFile)
	if err != nil {
		log.Fatal(err)
	}
	data, err := ioutil.ReadFile(scriptFile)
	if err != nil {
		fmt.Print(err)
	}

	md5str, err := getMD5AsString(data)
	// find each statement, checking annotations for up/down direction
	// and execute each of them in the current transaction.
	// Commits the transaction if successfully applied each statement and
	// records the version into the version table or returns an error and
	// rolls back the transaction.
	for _, query := range splitSQLStatements(f, direction) {
		if _, err = txn.Exec(query); err != nil {
			txn.Rollback()
			log.Fatalf("FAIL %s (%v), quitting migration.", filepath.Base(scriptFile), err)
			return err
		}
	}

	if err = FinalizeMigration(conf, txn, direction, v, md5str); err != nil {
		log.Fatalf("error finalizing migration %s, quitting. (%v)", filepath.Base(scriptFile), err)
	}

	return nil
}

func validateChecksum(conf *DBConf, db *sql.DB, scriptFile string, v int64) {

	data, err := ioutil.ReadFile(scriptFile)
	if err != nil {
		fmt.Print(err)
	}
	log.Println("procesing file :", scriptFile)
	md5str, err := getMD5AsString(data)
	log.Println("md5 string for file:", md5str)
	checksum, err := conf.Driver.Dialect.dbCheckSumQuery(db, v)
	log.Println("checksum from db:", checksum)
	if err != nil {
		fmt.Print(err)
	}

	if md5str != checksum {
		log.Fatal("checksum mismatch for file:", scriptFile)
	}
}

func getMD5AsString(data []byte) (string, error) {
	md5 := md5.Sum(data)
	md5str := fmt.Sprintf("%x\n", md5)

	return md5str, nil
}
