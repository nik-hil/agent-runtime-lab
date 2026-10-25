package tasklab

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// Receiver is a lab fixture, not a public service. afterFirstCommit is only used
// for deterministic crash injection; it runs AFTER file and directory fsync.
func Receiver(ledger Ledger, afterFirstCommit func()) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/health" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/commit" {
			http.NotFound(w, r)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var op Operation
		if err := decoder.Decode(&op); err != nil || op.Validate() != nil {
			http.Error(w, "invalid operation", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "trailing operation data", http.StatusBadRequest)
			return
		}
		receipt, err := ledger.Commit(op)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, ErrConflict) {
				status = http.StatusConflict
			}
			http.Error(w, err.Error(), status)
			return
		}
		if !receipt.Reused && afterFirstCommit != nil {
			afterFirstCommit()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(receipt)
	})
}
