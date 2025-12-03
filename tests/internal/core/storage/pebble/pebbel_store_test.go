package pebble_test

import (
	"path/filepath"
	"testing"

	"plastic-engine-core/internal/core/storage/pebble"
)

func TestPebbleStoreCRUD(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := pebble.NewPebbleStore(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	if err := store.Set("key", "value"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "value" {
		t.Fatalf("Get = %q, want %q", got, "value")
	}

	if err := store.Delete("key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := store.Get("key"); err == nil {
		t.Fatalf("expected error for deleted key, got nil")
	}
}
