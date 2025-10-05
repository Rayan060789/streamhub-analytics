
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type WatchEvent struct {
	UserID       string  `json:"user_id"`
	VideoID      string  `json:"video_id"`
	WatchSeconds float64 `json:"watch_seconds"`
	TS           string  `json:"ts"` // RFC3339 timestamp
}

var (
	writeMu   sync.Mutex
	logPath   = filepath.Join("data", "events.log")
)

func ensureDataDir() {
	if _, err := os.Stat("data"); os.IsNotExist(err) {
		if err := os.MkdirAll("data", 0o755); err != nil {
			log.Fatalf("failed to create data dir: %v", err)
		}
	}
}

func appendLine(line string) error {
	writeMu.Lock()
	defer writeMu.Unlock()

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	if _, err := w.WriteString(line + "\n"); err != nil {
		return err
	}
	return w.Flush()
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	// Accept single event OR array of events
	tok, err := decoder.Token()
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid json: %v", err), http.StatusBadRequest)
		return
	}

	switch v := tok.(type) {
	case json.Delim:
		if v == '{' {
			// single object
			var ev WatchEvent
			if err := decoder.Decode(&ev); err != nil && err.Error() != "EOF" {
				http.Error(w, fmt.Sprintf("decode err: %v", err), http.StatusBadRequest)
				return
			}
			if ev.TS == "" {
				ev.TS = time.Now().UTC().Format(time.RFC3339Nano)
			}
			b, _ := json.Marshal(ev)
			if err := appendLine(string(b)); err != nil {
				http.Error(w, fmt.Sprintf("write err: %v", err), http.StatusInternalServerError)
				return
			}
		} else if v == '[' {
			// array
			var events []WatchEvent
			if err := decoder.Decode(&events); err != nil {
				http.Error(w, fmt.Sprintf("decode array err: %v", err), http.StatusBadRequest)
				return
			}
			var sb strings.Builder
			for _, ev := range events {
				if ev.TS == "" {
					ev.TS = time.Now().UTC().Format(time.RFC3339Nano)
				}
				b, _ := json.Marshal(ev)
				sb.WriteString(string(b))
				sb.WriteString("\n")
			}
			if err := appendLine(strings.TrimRight(sb.String(), "\n")); err != nil {
				http.Error(w, fmt.Sprintf("write err: %v", err), http.StatusInternalServerError)
				return
			}
		}
	default:
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"accepted"}`))
}

func main() {
	ensureDataDir()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/events", eventsHandler)

	addr := ":8080"
	log.Printf("ingest server listening on %s", addr)
	log.Printf("POST events to http://localhost%v/events (single object or array)", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
