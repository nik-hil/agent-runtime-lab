package fleet

import (
	"context"
	"path/filepath"
	"testing"
)

func TestDrainStepRequiresDurableIntentAndNeverDispatchesQueuedWork(t *testing.T) {
	backend := newFake()
	c, store := opened(t, filepath.Join(t.TempDir(), "fleet.json"), backend, testConfig())
	defer store.Close()
	if err := c.Submit("alpha", "queued"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DrainStep(context.Background(), "queued"); err == nil {
		t.Fatal("drain step implicitly deleted work without drain intent")
	}
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DrainStep(context.Background(), "queued"); err != nil || backend.creates != 0 {
		t.Fatalf("drain dispatched queued work: creates=%d err=%v", backend.creates, err)
	}
}
