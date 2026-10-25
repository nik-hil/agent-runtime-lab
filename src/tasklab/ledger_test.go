package tasklab

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestLedgerRetryRestartAndConflicts(t *testing.T) {
	dir := t.TempDir()
	op := Operation{TaskID: "task-one", Key: "key-one", Payload: Artifact}
	first, err := (Ledger{dir}).Commit(op)
	if err != nil || first.Reused {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := (Ledger{dir}).Commit(op)
	if err != nil || !second.Reused || second.Record != first.Record {
		t.Fatalf("retry=%+v err=%v", second, err)
	}
	for _, conflict := range []Operation{{"task-two", op.Key, Artifact}, {op.TaskID, "key-two", Artifact}} {
		if _, err := (Ledger{dir}).Commit(conflict); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected conflict, got %v", err)
		}
	}
	if _, err := (Ledger{dir}).Commit(Operation{"task-two", "key-two", Artifact}); err != nil {
		t.Fatal(err)
	}
	records, err := (Ledger{dir}).Snapshot()
	if err != nil || len(records) != 2 {
		t.Fatalf("%+v %v", records, err)
	}
}

func TestConcurrentDuplicateCommits(t *testing.T) {
	dir := t.TempDir()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := (Ledger{dir}).Commit(Operation{"task", "key", Artifact}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	records, err := (Ledger{dir}).Snapshot()
	if err != nil || len(records) != 1 {
		t.Fatalf("duplicates: %+v %v", records, err)
	}
}

func TestLedgerFailsClosedOnCorruption(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ledger.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := (Ledger{dir}).Commit(Operation{"task", "key", Artifact}); err == nil {
		t.Fatal("accepted corrupt ledger")
	}
}
