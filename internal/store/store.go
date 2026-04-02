package store
import ("crypto/sha256";"database/sql";"encoding/hex";"encoding/json";"fmt";"os";"path/filepath";"strings";"time";_ "modernc.org/sqlite")
type DB struct{ db *sql.DB }
type Event struct {
	ID        string            `json:"id"`
	Seq       int               `json:"seq"`
	Actor     string            `json:"actor"`
	Action    string            `json:"action"`
	Resource  string            `json:"resource,omitempty"`
	Detail    string            `json:"detail,omitempty"`
	Meta      map[string]string `json:"meta,omitempty"`
	Hash      string            `json:"hash"`
	PrevHash  string            `json:"prev_hash"`
	CreatedAt string            `json:"created_at"`
}
func Open(dataDir string) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil { return nil, err }
	dsn := filepath.Join(dataDir, "deposition.db") + "?_journal_mode=WAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil { return nil, err }
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, seq INTEGER UNIQUE, actor TEXT DEFAULT '', action TEXT NOT NULL, resource TEXT DEFAULT '', detail TEXT DEFAULT '', meta_json TEXT DEFAULT '{}', hash TEXT NOT NULL, prev_hash TEXT DEFAULT '', created_at TEXT DEFAULT (datetime('now')))`,
		`CREATE INDEX IF NOT EXISTS idx_events_actor ON events(actor)`,
		`CREATE INDEX IF NOT EXISTS idx_events_action ON events(action)`,
		`CREATE INDEX IF NOT EXISTS idx_events_resource ON events(resource)`,
	} { if _, err := db.Exec(q); err != nil { return nil, fmt.Errorf("migrate: %w", err) } }
	return &DB{db: db}, nil
}
func (d *DB) Close() error { return d.db.Close() }
func genID() string { return fmt.Sprintf("%d", time.Now().UnixNano()) }
func now() string { return time.Now().UTC().Format(time.RFC3339) }
func hashEvent(seq int, actor, action, resource, detail, prevHash, ts string) string {
	data := fmt.Sprintf("%d|%s|%s|%s|%s|%s|%s", seq, actor, action, resource, detail, prevHash, ts)
	h := sha256.Sum256([]byte(data)); return hex.EncodeToString(h[:])
}
func (d *DB) Append(e *Event) error {
	e.ID = genID(); e.CreatedAt = now()
	if e.Meta == nil { e.Meta = map[string]string{} }
	var maxSeq int; var prevHash string
	d.db.QueryRow(`SELECT COALESCE(MAX(seq),0), COALESCE((SELECT hash FROM events ORDER BY seq DESC LIMIT 1),'genesis') FROM events`).Scan(&maxSeq, &prevHash)
	e.Seq = maxSeq + 1; e.PrevHash = prevHash
	e.Hash = hashEvent(e.Seq, e.Actor, e.Action, e.Resource, e.Detail, e.PrevHash, e.CreatedAt)
	mj, _ := json.Marshal(e.Meta)
	_, err := d.db.Exec(`INSERT INTO events (id,seq,actor,action,resource,detail,meta_json,hash,prev_hash,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)`,
		e.ID, e.Seq, e.Actor, e.Action, e.Resource, e.Detail, string(mj), e.Hash, e.PrevHash, e.CreatedAt)
	return err
}
type EventFilter struct { Actor string; Action string; Resource string; Search string; After string; Before string; Limit int; Offset int }
func (d *DB) Query(f EventFilter) ([]Event, int) {
	where := []string{"1=1"}; args := []any{}
	if f.Actor != "" { where = append(where, "actor=?"); args = append(args, f.Actor) }
	if f.Action != "" { where = append(where, "action=?"); args = append(args, f.Action) }
	if f.Resource != "" { where = append(where, "resource=?"); args = append(args, f.Resource) }
	if f.Search != "" { where = append(where, "(detail LIKE ? OR resource LIKE ?)"); s := "%" + f.Search + "%"; args = append(args, s, s) }
	if f.After != "" { where = append(where, "created_at>=?"); args = append(args, f.After) }
	if f.Before != "" { where = append(where, "created_at<=?"); args = append(args, f.Before) }
	w := strings.Join(where, " AND "); var total int
	d.db.QueryRow("SELECT COUNT(*) FROM events WHERE "+w, args...).Scan(&total)
	if f.Limit <= 0 { f.Limit = 50 }
	q := fmt.Sprintf("SELECT id,seq,actor,action,resource,detail,meta_json,hash,prev_hash,created_at FROM events WHERE %s ORDER BY seq DESC LIMIT ? OFFSET ?", w)
	args = append(args, f.Limit, f.Offset)
	rows, _ := d.db.Query(q, args...); if rows == nil { return nil, 0 }; defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event; var mj string
		rows.Scan(&e.ID, &e.Seq, &e.Actor, &e.Action, &e.Resource, &e.Detail, &mj, &e.Hash, &e.PrevHash, &e.CreatedAt)
		json.Unmarshal([]byte(mj), &e.Meta); out = append(out, e)
	}
	return out, total
}
func (d *DB) Verify() (bool, int, string) {
	rows, _ := d.db.Query(`SELECT seq,actor,action,resource,detail,hash,prev_hash,created_at FROM events ORDER BY seq ASC`)
	if rows == nil { return true, 0, "" }; defer rows.Close()
	count := 0; prevHash := "genesis"
	for rows.Next() {
		var seq int; var actor, action, resource, detail, hash, ph, ts string
		rows.Scan(&seq, &actor, &action, &resource, &detail, &hash, &ph, &ts)
		if ph != prevHash { return false, seq, "prev_hash mismatch at seq " + fmt.Sprintf("%d", seq) }
		expected := hashEvent(seq, actor, action, resource, detail, ph, ts)
		if hash != expected { return false, seq, "hash mismatch at seq " + fmt.Sprintf("%d", seq) }
		prevHash = hash; count++
	}
	return true, count, ""
}
type Stats struct { Events int `json:"events"`; Actors int `json:"actors"`; Verified bool `json:"verified"` }
func (d *DB) Stats() Stats {
	var s Stats; d.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&s.Events)
	d.db.QueryRow(`SELECT COUNT(DISTINCT actor) FROM events`).Scan(&s.Actors)
	s.Verified, _, _ = d.Verify(); return s
}
