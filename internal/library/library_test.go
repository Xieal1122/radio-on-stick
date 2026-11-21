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

func TestSidecarOverridesFilenameMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "upload-1795229500234185000.mp3")
	if err := os.WriteFile(path, []byte("not real audio"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteMeta(path, FileMeta{
		Title: "小さな海", Artist: "結束バンド", Album: "本地上传",
		DurationMS: 223866, Nickname: "杰森",
	}); err != nil {
		t.Fatal(err)
	}
	index := New()
	if _, err := index.Scan([]string{dir}, 100); err != nil {
		t.Fatal(err)
	}
	results := index.Search("小さな海", 10)
	if len(results) != 1 {
		t.Fatalf("results = %#v", results)
	}
	track := results[0]
	if track.Title != "小さな海" || track.Artist != "結束バンド" {
		t.Fatalf("sidecar metadata not applied: %#v", track)
	}
	if track.DurationMS != 223866 {
		t.Fatalf("duration = %d", track.DurationMS)
	}
	// A sidecar means /api/upload wrote the file: it is a one-shot upload and
	// must stay distinguishable from the permanent library.
	if track.Source != "upload" {
		t.Fatalf("source = %q, want upload", track.Source)
	}
}

func TestAddFileKeepsScannedTracks(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.mp3")
	if err := os.WriteFile(first, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	index := New()
	if count, err := index.Scan([]string{dir}, 100); err != nil || count != 1 {
		t.Fatalf("scan count=%d err=%v", count, err)
	}
	second := filepath.Join(dir, "second.flac")
	if err := os.WriteFile(second, []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	track, ok := index.AddFile(second)
	if !ok {
		t.Fatal("AddFile rejected a supported extension")
	}
	if index.Count() != 2 {
		t.Fatalf("count = %d", index.Count())
	}
	if _, found := index.Get(track.ID); !found {
		t.Fatal("added track not retrievable")
	}
	if len(index.List(10)) != 2 {
		t.Fatalf("list = %#v", index.List(10))
	}
}

func TestPickTagIsCaseInsensitive(t *testing.T) {
	tags := map[string]string{"TITLE": " 小さな海 ", "ARTIST": "結束バンド"}
	if got := pickTag(tags, "title"); got != "小さな海" {
		t.Fatalf("title = %q", got)
	}
	if got := pickTag(tags, "artist", "album_artist"); got != "結束バンド" {
		t.Fatalf("artist = %q", got)
	}
	if got := pickTag(tags, "album"); got != "" {
		t.Fatalf("album = %q", got)
	}
}

func TestParseClockMS(t *testing.T) {
	cases := map[string]int64{
		"00:03:43.87": 223870,
		"03:43.87":    223870,
		"42.5":        42500,
		"":            0,
		"garbage":     0,
	}
	for input, want := range cases {
		if got := parseClockMS(input); got != want {
			t.Fatalf("parseClockMS(%q) = %d, want %d", input, got, want)
		}
	}
}
