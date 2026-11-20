package library

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSearchAndTitle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "01 - Test Song.mp3")
	if err := os.WriteFile(path, []byte("not real audio"), 0o600); err != nil {
		t.Fatal(err)
	}
	index := New()
	count, err := index.Scan([]string{dir}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count = %d", count)
	}
	results := index.Search("test", 10)
	if len(results) != 1 || results[0].Title != "01 - Test Song" {
		t.Fatalf("results = %#v", results)
	}
	if _, ok := index.Get(results[0].ID); !ok {
		t.Fatal("track missing by id")
	}
}
