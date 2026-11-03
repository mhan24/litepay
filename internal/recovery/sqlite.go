// Package recovery creates SQLite-consistent, verified snapshots without copying a live WAL database.
package recovery

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/glebarez/go-sqlite"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func openReadOnly(path string) (*sql.DB, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	uri := (&url.URL{Scheme: "file", Path: absolute}).String() + "?mode=ro&_pragma=busy_timeout(5000)"
	return sql.Open("sqlite", uri)
}
func Verify(ctx context.Context, path string) error {
	db, err := openReadOnly(path)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
			return fmt.Errorf("SQLite integrity check failed")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("missing integrity result")
	}
	return nil
}

// Snapshot refuses to overwrite any destination. Restore by stopping the application,
// preserving its current volume, and installing a verified snapshot at a new path.
func Snapshot(ctx context.Context, source, destination string) error {
	f, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = os.Remove(destination)
		}
	}()
	db, err := openReadOnly(source)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "VACUUM INTO '"+strings.ReplaceAll(destination, "'", "''")+"'"); err != nil {
		return err
	}
	if err := Verify(ctx, destination); err != nil {
		return err
	}
	out, err := os.OpenFile(destination, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	if err := out.Sync(); err != nil {
		return err
	}
	success = true
	return nil
}
