package station

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Xieal1122/radio-go/internal/library"
)

func TestAdvanceHookRetiresSkippedAndFinishedTracks(t *testing.T) {
	s := New(library.New(), "test")
	s.current = library.Track{ID: "one", DurationMS: 1000}
	s.queue = []library.Track{{ID: "two", DurationMS: 1000}}
	var retired []string
	s.SetAdvanceHook(func(track library.Track) { retired = append(retired, track.ID) })
	s.Skip()
	if len(retired) != 1 || retired[0] != "one" {
		t.Fatalf("skip did not retire the previous track: %#v", retired)
	}
	s.current = library.Track{ID: "three", DurationMS: 10}
	s.started = time.Now().Add(-time.Second)
	s.queue = append(s.queue, library.Track{ID: "four", DurationMS: 1000})
	s.Tick()
	if len(retired) != 2 || retired[1] != "three" {
		t.Fatalf("natural end did not retire the track: %#v", retired)
	}
}

func TestAdvanceHookKeepsTrackThatIsImmediatelyReplayed(t *testing.T) {
	s := New(library.New(), "test")
	s.current = library.Track{ID: "only", DurationMS: 1000}
	s.queue = []library.Track{{ID: "only", DurationMS: 1000}}
	fired := 0
	s.SetAdvanceHook(func(library.Track) { fired++ })
	s.Skip()
	if fired != 0 {
		t.Fatalf("hook retired a track that is playing again: %d", fired)
	}
}

func TestPauseFreezesAuthoritativePosition(t *testing.T) {
	s := New(library.New(), "test")
	s.current = library.Track{ID: "track", Title: "Track", DurationMS: 60000}
	s.started = time.Now().Add(-3 * time.Second)
	s.seq = 1
	s.Pause(true)
	first := s.PositionMS()
	time.Sleep(30 * time.Millisecond)
	second := s.PositionMS()
	if first != second {
		t.Fatalf("position advanced while paused: %d -> %d", first, second)
	}
	s.Pause(false)
	time.Sleep(20 * time.Millisecond)
	if resumed := s.PositionMS(); resumed <= first {
		t.Fatalf("position did not resume: %d -> %d", first, resumed)
	}
}

func TestVotesResetWhenTrackEndsNaturally(t *testing.T) {
	s := New(library.New(), "test")
	s.current = library.Track{ID: "one", Title: "One", DurationMS: 10}
	s.started = time.Now().Add(-time.Second)
	s.queue = []library.Track{{ID: "two", Title: "Two", DurationMS: 1000}}
	s.votes = map[string]bool{"a": true, "b": true}
	s.Tick()
	if s.current.ID != "two" {
		t.Fatalf("track did not advance: %#v", s.current)
	}
	if len(s.votes) != 0 {
		t.Fatalf("votes survived natural advance: %#v", s.votes)
	}
}

func TestVotesResetOnManualSkip(t *testing.T) {
	s := New(library.New(), "test")
	s.current = library.Track{ID: "one", Title: "One", DurationMS: 1000}
	s.queue = []library.Track{{ID: "two", Title: "Two", DurationMS: 1000}}
	s.votes = map[string]bool{"a": true}
	s.Skip()
	if s.current.ID != "two" {
		t.Fatalf("track did not advance after skip: %#v", s.current)
	}
	if len(s.votes) != 0 {
		t.Fatalf("votes survived manual skip: %#v", s.votes)
	}
}

func TestQueueCancelRequiresOwner(t *testing.T) {
	s := New(library.New(), "test")
	s.Request(library.Track{ID: "track", Title: "Track", DurationMS: 1000}, "alice", "client-a")
	queue := s.Queue()
	if len(queue) != 1 || queue[0].QueueID == 0 {
		t.Fatalf("request was not queued: %#v", queue)
	}
	if s.Cancel(queue[0].QueueID, "client-b") {
		t.Fatal("another client cancelled the request")
	}
	if !s.Cancel(queue[0].QueueID, "client-a") {
		t.Fatal("owner could not cancel the request")
	}
}

func TestStatePersistenceIncludesHistoryAndMessages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := New(library.New(), "test")
	s.SetPersistPath(path)
	s.AddMessage("alice", "hello")
	s.Request(library.Track{ID: "track", Title: "Track", DurationMS: 1000}, "alice", "client-a")
	if err := os.WriteFile(path, mustJSON(s), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded := New(library.New(), "test")
	loaded.SetPersistPath(path)
	if got := len(loaded.Messages()); got != 1 {
		t.Fatalf("messages not restored: %d", got)
	}
	if got := len(loaded.History()); got == 0 {
		t.Fatal("history not restored")
	}
	if got := len(loaded.Queue()); got == 0 {
		t.Fatal("queue not restored")
	}
}

func mustJSON(s *State) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	raw, err := os.ReadFile(s.persistPath)
	if err != nil {
		panic(err)
	}
	return raw
}

func TestRotationRefillsWhenQueueDrains(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.mp3", "b.mp3", "c.mp3"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	index := library.New()
	if _, err := index.Scan([]string{dir}, 10); err != nil {
		t.Fatal(err)
	}
	s := New(index, "test")
	s.Rebuild()
	seen := map[string]bool{}
	for step := 0; step < 7; step++ {
		s.Skip()
		if s.current.ID == "" {
			t.Fatalf("station went silent after %d skips", step+1)
		}
		seen[s.current.ID] = true
	}
	if len(seen) < 3 {
		t.Fatalf("rotation never covered the library: %#v", seen)
	}
}
