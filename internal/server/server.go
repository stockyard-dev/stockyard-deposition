package server
import ("encoding/json";"log";"net/http";"strconv";"github.com/stockyard-dev/stockyard-deposition/internal/store")
type Server struct { db *store.DB; mux *http.ServeMux }
func New(db *store.DB) *Server {
	s := &Server{db: db, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/events", s.append)
	s.mux.HandleFunc("GET /api/events", s.query)
	s.mux.HandleFunc("GET /api/verify", s.verify)
	s.mux.HandleFunc("GET /api/stats", s.stats)
	s.mux.HandleFunc("GET /api/health", s.health)
	s.mux.HandleFunc("GET /ui", s.dashboard); s.mux.HandleFunc("GET /ui/", s.dashboard); s.mux.HandleFunc("GET /", s.root)
	return s
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }
func writeJSON(w http.ResponseWriter, code int, v any) { w.Header().Set("Content-Type","application/json"); w.WriteHeader(code); json.NewEncoder(w).Encode(v) }
func writeErr(w http.ResponseWriter, code int, msg string) { writeJSON(w, code, map[string]string{"error": msg}) }
func (s *Server) root(w http.ResponseWriter, r *http.Request) { if r.URL.Path != "/" { http.NotFound(w, r); return }; http.Redirect(w, r, "/ui", http.StatusFound) }
func (s *Server) append(w http.ResponseWriter, r *http.Request) {
	var e store.Event; json.NewDecoder(r.Body).Decode(&e)
	if e.Action == "" { writeErr(w, 400, "action required"); return }
	if err := s.db.Append(&e); err != nil { writeErr(w, 500, err.Error()); return }
	writeJSON(w, 201, e)
}
func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query(); limit, _ := strconv.Atoi(q.Get("limit")); offset, _ := strconv.Atoi(q.Get("offset"))
	f := store.EventFilter{Actor:q.Get("actor"),Action:q.Get("action"),Resource:q.Get("resource"),Search:q.Get("search"),After:q.Get("after"),Before:q.Get("before"),Limit:limit,Offset:offset}
	events, total := s.db.Query(f)
	writeJSON(w, 200, map[string]any{"events": orEmpty(events), "total": total})
}
func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	ok, count, msg := s.db.Verify()
	writeJSON(w, 200, map[string]any{"valid": ok, "events_checked": count, "error": msg})
}
func (s *Server) stats(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.db.Stats()) }
func (s *Server) health(w http.ResponseWriter, r *http.Request) { st := s.db.Stats(); writeJSON(w, 200, map[string]any{"status":"ok","service":"deposition","events":st.Events,"verified":st.Verified}) }
func orEmpty[T any](s []T) []T { if s == nil { return []T{} }; return s }
func init() { log.SetFlags(log.LstdFlags | log.Lshortfile) }
