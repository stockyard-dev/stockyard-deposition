package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/stockyard-dev/stockyard-deposition/internal/server"
	"github.com/stockyard-dev/stockyard-deposition/internal/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9310"
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./deposition-data"
	}

	db, err := store.Open(dataDir)
	if err != nil {
		log.Fatalf("deposition: open database: %v", err)
	}
	defer db.Close()

	srv := server.New(db)

	fmt.Printf("\n  Deposition — Self-hosted tamper-evident audit log\n")
	fmt.Printf("  ─────────────────────────────────\n")
	fmt.Printf("  Dashboard:  http://localhost:%s/ui\n", port)
	fmt.Printf("  API:        http://localhost:%s/api\n", port)
	fmt.Printf("  Data:       %s\n", dataDir)
	fmt.Printf("  ─────────────────────────────────\n\n")

	log.Printf("deposition: listening on :%s", port)
	if err := http.ListenAndServe(":"+port, srv); err != nil {
		log.Fatalf("deposition: %v", err)
	}
}
