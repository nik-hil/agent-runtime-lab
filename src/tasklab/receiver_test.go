package tasklab

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestReceiverConcurrentNewOperationAndConflict(t *testing.T) {
	l := Ledger{t.TempDir()}
	h := Receiver(l, nil)
	op := Operation{"task", "key", Artifact}
	body, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/commit", bytes.NewReader(body)))
			if w.Code != http.StatusOK {
				t.Errorf("commit: %d %s", w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	records, err := l.Snapshot()
	if err != nil || len(records) != 1 {
		t.Fatalf("ledger %+v %v", records, err)
	}
	op.TaskID = "other-task"
	body, _ = json.Marshal(op)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/commit", bytes.NewReader(body)))
	if w.Code != http.StatusConflict {
		t.Fatalf("collision: %d", w.Code)
	}
}

func TestReceiverCommitPrecedesAcknowledgement(t *testing.T) {
	l := Ledger{t.TempDir()}
	const injected = "crash-after-persistence"
	h := Receiver(l, func() { panic(injected) })
	op := Operation{"task", "key", Artifact}
	body, _ := json.Marshal(op)
	w := httptest.NewRecorder()
	func() {
		defer func() {
			if r := recover(); r != injected {
				t.Errorf("crash hook: %v", r)
			}
		}()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/commit", bytes.NewReader(body)))
		t.Error("crash hook did not interrupt acknowledgement")
	}()
	if w.Body.Len() != 0 {
		t.Fatal("acknowledgement sent before crash")
	}
	records, err := (Ledger{l.Directory}).Snapshot()
	if err != nil || len(records) != 1 || records[0].Operation != op {
		t.Fatalf("commit lost: %+v %v", records, err)
	}
}

func TestReceiverRejectsInvalidRequests(t *testing.T) {
	l := Ledger{t.TempDir()}
	for _, body := range []string{`{}`, `{"task_id":"../bad"}`, `{"task_id":"task","idempotency_key":"key","payload":"bad"}`} {
		w := httptest.NewRecorder()
		Receiver(l, nil).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/commit", bytes.NewBufferString(body)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("accepted %s", body)
		}
	}
	records, err := l.Snapshot()
	if err != nil || len(records) != 0 {
		t.Fatalf("invalid request committed: %+v %v", records, err)
	}
}
