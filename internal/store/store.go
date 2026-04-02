package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type DB struct { db *sql.DB }

type AuditEntry struct {
	ID           string   `json:"id"`
	Actor        string   `json:"actor"`
	Action       string   `json:"action"`
	Resource     string   `json:"resource"`
	Detail       string   `json:"detail"`
	Hash         string   `json:"hash"`
	CreatedAt    string   `json:"created_at"`
}

func Open(dataDir string) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	dsn := filepath.Join(dataDir, "deposition.db") + "?_journal_mode=WAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS auditentrys (
			id TEXT PRIMARY KEY,\n\t\t\tactor TEXT DEFAULT '',\n\t\t\taction TEXT DEFAULT '',\n\t\t\tresource TEXT DEFAULT '',\n\t\t\tdetail TEXT DEFAULT '',\n\t\t\thash TEXT DEFAULT '',
			created_at TEXT DEFAULT (datetime('now'))
		)`)
	if err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error { return d.db.Close() }

func genID() string { return fmt.Sprintf("%d", time.Now().UnixNano()) }

func (d *DB) Create(e *AuditEntry) error {
	e.ID = genID()
	e.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	_, err := d.db.Exec(`INSERT INTO auditentrys (id, actor, action, resource, detail, hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.Actor, e.Action, e.Resource, e.Detail, e.Hash, e.CreatedAt)
	return err
}

func (d *DB) Get(id string) *AuditEntry {
	row := d.db.QueryRow(`SELECT id, actor, action, resource, detail, hash, created_at FROM auditentrys WHERE id=?`, id)
	var e AuditEntry
	if err := row.Scan(&e.ID, &e.Actor, &e.Action, &e.Resource, &e.Detail, &e.Hash, &e.CreatedAt); err != nil {
		return nil
	}
	return &e
}

func (d *DB) List() []AuditEntry {
	rows, err := d.db.Query(`SELECT id, actor, action, resource, detail, hash, created_at FROM auditentrys ORDER BY created_at DESC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var result []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.Actor, &e.Action, &e.Resource, &e.Detail, &e.Hash, &e.CreatedAt); err != nil {
			continue
		}
		result = append(result, e)
	}
	return result
}

func (d *DB) Delete(id string) error {
	_, err := d.db.Exec(`DELETE FROM auditentrys WHERE id=?`, id)
	return err
}

func (d *DB) Count() int {
	var n int
	d.db.QueryRow(`SELECT COUNT(*) FROM auditentrys`).Scan(&n)
	return n
}
