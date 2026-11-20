package station

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"path/filepath"
	"sync"
	"time"

	"radio-on-stick/station-go/internal/library"
)

type Message struct {
	ID        string `json:"id"`
	Nickname  string `json:"nickname"`
	Body      string `json:"body"`
	CreatedAt int64  `json:"created_at"`
}

type History struct {
	ID                string `json:"id"`
	TrackID           string `json:"track_id"`
	Title             string `json:"title"`
	Artist            string `json:"artist"`
	CoverURL          string `json:"cover_url"`
	Nickname          string `json:"nickname"`
	CreatedAt         int64  `json:"created_at"`
	DurationMS        int64  `json:"duration_ms"`
	DurationEstimated bool   `json:"duration_estimated"`
	Source            string `json:"source,omitempty"`
	SourceID          string `json:"source_id,omitempty"`
	URL               string `json:"url,omitempty"`
}

type savedTrack struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Artist     string `json:"artist"`
	Album      string `json:"album"`
	DurationMS int64  `json:"duration_ms"`
	Format     string `json:"format,omitempty"`
	Path       string `json:"path,omitempty"`
	URL        string `json:"url,omitempty"`
	Source     string `json:"source,omitempty"`
	SourceID   string `json:"source_id,omitempty"`
	CoverURL   string `json:"cover_url,omitempty"`
}

func saveTrack(track library.Track) savedTrack {
	return savedTrack{
		ID: track.ID, Title: track.Title, Artist: track.Artist, Album: track.Album,
		DurationMS: track.DurationMS, Format: track.Format, Path: track.Path, URL: track.URL,
		Source: track.Source, SourceID: track.SourceID, CoverURL: track.CoverURL,
	}
}

func loadTrack(track savedTrack) library.Track {
	return library.Track{
		ID: track.ID, Title: track.Title, Artist: track.Artist, Album: track.Album,
		DurationMS: track.DurationMS, Format: track.Format, Path: track.Path, URL: track.URL,
		Source: track.Source, SourceID: track.SourceID, CoverURL: track.CoverURL,
	}
}

type QueueEntry struct {
	QueueID   int64  `json:"queue_id"`
	TrackID   string `json:"track_id"`
	Title     string `json:"title"`
	Artist    string `json:"artist"`
	CoverURL  string `json:"cover_url"`
	Nickname  string `json:"nickname"`
	Status    string `json:"status"`
	CreatedAt         int64  `json:"created_at"`
	PinCount          int      `json:"pin_count"`
	PinClients        []string `json:"pin_clients,omitempty"`
	Priority          int      `json:"priority"`
	PinnedByMe        bool     `json:"pinned_by_me"`
	DurationMS        int64    `json:"duration_ms"`
	DurationEstimated bool     `json:"duration_estimated"`
	ClientKey         string   `json:"-"`
	CanCancel         bool     `json:"can_cancel"`
}

type State struct {
	mu       sync.RWMutex
	index    *library.Index
	name     string
	current  library.Track
	queue    []library.Track
	seq            int
	started        time.Time
	paused         bool
	pausedPosition int64
	audioRev       int
	messages []Message
	history  []History
	requests []QueueEntry
	nextID   int
	queueID  int64
	votes       map[string]bool
	settings    map[string]any
	policy      map[string]any
	persistPath string
	listeners   int
	onAdvance   func(library.Track)
}

func New(index *library.Index, name string) *State {
	return &State{
		index: index, name: name, votes: map[string]bool{},
		settings: map[string]any{
			"station_name": name, "subtitle": "local library", "notice": "",
			"page_title": name, "background_interval_sec": 300,
		},
		policy: map[string]any{
			"max_listeners": 0, "mp3_upload_only": false, "mp3_remote_only": false,
			"requests_enabled": true, "uploads_enabled": true, "messages_enabled": true,
		},
	}
}

func (s *State) Rebuild() {
	s.mu.Lock()
	defer s.mu.Unlock()
	tracks := s.index.List(50000)
	valid := make(map[string]bool, len(tracks))
	queue := make([]library.Track, 0, len(tracks))
	for _, track := range tracks {
		valid[track.ID] = true
		if track.ID == s.current.ID {
			continue
		}
		queue = append(queue, track)
	}
	// Keep pending requests only if their track still exists.
	kept := s.requests[:0]
	for _, entry := range s.requests {
		if valid[entry.TrackID] {
			kept = append(kept, entry)
		}
	}
	s.requests = kept
	// Keep a shuffled library order so the next track is known in advance
	// (metadata preload) while the rotation itself is still random.
	rand.Shuffle(len(queue), func(a, b int) {
		queue[a], queue[b] = queue[b], queue[a]
	})
	s.queue = queue
	if s.current.ID == "" {
		s.advanceLocked()
	}
	// A restored remote track is not in the local index. Preserve it unless
	// the admin explicitly deletes the current track (adminTrackDelete calls
	// Skip first).
}

func (s *State) SetAdvanceHook(hook func(library.Track)) {
	s.mu.Lock()
	s.onAdvance = hook
	s.mu.Unlock()
}

func (s *State) advanceLocked() {
	if s.paused || s.current.ID != "" {
		return
	}
	old := s.current
	// A new track always starts a fresh vote round, whether it was selected
	// manually, by vote, or because the previous track ended naturally.
	s.votes = map[string]bool{}
	s.reorderQueueLocked()
	if len(s.queue) == 0 {
		return
	}
	nextIndex := 0
	// The library queue is shuffled when rebuilt. Pending requests are moved to
	// the front by reorderQueueLocked; otherwise the first auto track is the
	// same one exposed as next_auto for preloading.
	s.current = s.queue[nextIndex]
	s.queue = append(s.queue[:nextIndex], s.queue[nextIndex+1:]...)
	if s.current.CoverURL == "" {
		if latest, ok := s.index.Get(s.current.ID); ok {
			if latest.CoverURL != "" { s.current.CoverURL = latest.CoverURL }
			if s.current.Title == "" { s.current.Title = latest.Title }
			if s.current.Artist == "" { s.current.Artist = latest.Artist }
			if s.current.Album == "" { s.current.Album = latest.Album }
		}
	}
	s.started = time.Now()
	s.pausedPosition = 0
	s.seq++
	s.audioRev++
	for index := range s.requests {
		switch {
		case s.requests[index].TrackID == s.current.ID:
			s.requests[index].Status = "playing"
		case s.requests[index].Status == "playing":
			s.requests[index].Status = "played"
		}
	}
	s.saveLocked()
	if old.ID != "" && s.onAdvance != nil {
		// Upload lifecycle cleanup runs after the new track is committed and
		// never touches paths outside the configured uploads directory.
		s.onAdvance(old)
	}
}

func (s *State) tickLocked(now time.Time) {
	if s.paused || s.current.ID == "" {
		return
	}
	if s.current.DurationMS <= 0 {
		return
	}
	if now.Sub(s.started) >= time.Duration(s.current.DurationMS)*time.Millisecond {
		s.current = library.Track{}
		s.started = time.Time{}
		s.advanceLocked()
	}
}

func (s *State) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickLocked(time.Now())
}

func (s *State) Pause(paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if paused {
		if s.paused {
			return
		}
		if s.current.ID != "" && !s.started.IsZero() {
			position := time.Since(s.started).Milliseconds()
			if s.current.DurationMS > 0 && position > s.current.DurationMS {
				position = s.current.DurationMS
			}
			if position < 0 {
				position = 0
			}
			s.pausedPosition = position
		}
		s.paused = true
		s.saveLocked()
		return
	}
	if !s.paused {
		return
	}
	now := time.Now()
	s.paused = false
	if s.current.ID != "" && s.pausedPosition > 0 {
		s.started = now.Add(-time.Duration(s.pausedPosition) * time.Millisecond)
	}
	if s.current.ID == "" {
		s.advanceLocked()
	} else {
		s.saveLocked()
	}
}

func (s *State) Skip() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votes = map[string]bool{}
	s.current = library.Track{}
	s.started = time.Time{}
	s.pausedPosition = 0
	s.advanceLocked()
}

func (s *State) Request(track library.Track, nickname, clientKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	// History is unique by track: requesting the same song again refreshes its
	// timestamp and moves it to the top instead of creating a duplicate row.
	entry := History{
		ID: fmt.Sprintf("h%d", s.nextID), TrackID: track.ID, Title: track.Title,
		Artist: track.Artist, CoverURL: track.CoverURL, Nickname: nickname,
		CreatedAt: time.Now().UnixMilli(), DurationMS: track.DurationMS,
		DurationEstimated: track.DurationMS <= 0,
		Source: track.Source, SourceID: track.SourceID, URL: track.URL,
	}
	history := make([]History, 0, len(s.history)+1)
	history = append(history, entry)
	for _, old := range s.history {
		if old.TrackID != track.ID {
			history = append(history, old)
		}
	}
	if len(history) > 200 {
		history = history[:200]
	}
	s.history = history
	if s.current.ID == track.ID {
		s.saveLocked()
		return
	}
	for _, entry := range s.requests {
		if entry.TrackID == track.ID && entry.Status == "pending" {
			return
		}
	}
	s.queue = append([]library.Track{track}, s.queue...)
	s.queueID++
	s.requests = append(s.requests, QueueEntry{
		QueueID: s.queueID, TrackID: track.ID, Title: track.Title,
		Artist: track.Artist, CoverURL: track.CoverURL, Nickname: nickname, Status: "pending",
		CreatedAt: time.Now().UnixMilli(), ClientKey: clientKey,
		DurationMS: track.DurationMS, DurationEstimated: track.DurationMS <= 0,
	})
	s.reorderQueueLocked()
	s.advanceLocked()
	s.saveLocked()
}

func (s *State) PositionMS() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current.ID == "" {
		return 0
	}
	if s.paused {
		return s.pausedPosition
	}
	if s.started.IsZero() {
		return 0
	}
	position := time.Since(s.started).Milliseconds()
	if s.current.DurationMS > 0 && position > s.current.DurationMS {
		position = s.current.DurationMS
	}
	if position < 0 {
		position = 0
	}
	return position
}

func (s *State) Revision() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("%d:%d", s.seq, s.audioRev)
}

func (s *State) TrackByID(id string) (library.Track, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current.ID == id {
		return s.current, true
	}
	for _, track := range s.queue {
		if track.ID == id {
			return track, true
		}
	}
	return library.Track{}, false
}

func (s *State) HistoryByTrackID(id string) (History, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, item := range s.history {
		if item.TrackID == id {
			return item, true
		}
	}
	return History{}, false
}

func (s *State) UpdateTrackMetadata(track library.Track) {
	s.mu.Lock()
	defer s.mu.Unlock()
	merge := func(target *library.Track) {
		if track.Title != "" { target.Title = track.Title }
		if track.Artist != "" { target.Artist = track.Artist }
		if track.Album != "" { target.Album = track.Album }
		if track.DurationMS > 0 { target.DurationMS = track.DurationMS }
		if track.CoverURL != "" { target.CoverURL = track.CoverURL }
		if track.Source != "" { target.Source = track.Source }
		if track.SourceID != "" { target.SourceID = track.SourceID }
		if track.URL != "" { target.URL = track.URL }
	}
	if s.current.ID == track.ID { merge(&s.current) }
	for index := range s.queue {
		if s.queue[index].ID == track.ID { merge(&s.queue[index]) }
	}
	s.saveLocked()
}

func (s *State) SetTrackAudio(id, url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current.ID == id {
		s.current.URL = url
		s.saveLocked()
		return true
	}
	for index := range s.queue {
		if s.queue[index].ID == id {
			s.queue[index].URL = url
			s.saveLocked()
			return true
		}
	}
	return false
}

func (s *State) Current() library.Track {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

func (s *State) Snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickLocked(time.Now())
	return s.SnapshotLocked()
}

func (s *State) SnapshotLocked() map[string]any {
	now := time.Now()
	position := int64(0)
	startedMS := int64(0)
	if s.current.ID != "" {
		if s.paused {
			position = s.pausedPosition
			startedMS = now.UnixMilli() - position
		} else {
			if s.started.IsZero() {
				s.started = now
			}
			startedMS = s.started.UnixMilli()
			position = now.Sub(s.started).Milliseconds()
			if s.current.DurationMS > 0 && position > s.current.DurationMS {
				position = s.current.DurationMS
			}
		}
	}
	entry := func(track library.Track) map[string]any {
		if track.ID == "" {
			return nil
		}
		source := track.Source
		if source == "" {
			source = "local"
		}
		coverURL := track.CoverURL
		if coverURL == "" {
			if latest, ok := s.index.Get(track.ID); ok {
				coverURL = latest.CoverURL
				if track.Title == "" { track.Title = latest.Title }
				if track.Artist == "" { track.Artist = latest.Artist }
				if track.Album == "" { track.Album = latest.Album }
			}
		}
		direct := "/native/" + track.ID
		return map[string]any{
			"id": track.ID, "source": source, "source_id": track.SourceID, "title": track.Title,
			"artist": track.Artist, "album": track.Album,
			"duration_ms": track.DurationMS, "cover_url": coverURL,
			"lyrics_url": "/api/lyrics/" + track.ID,
			"audio": map[string]any{"direct": direct},
		}
	}
	next := map[string]any(nil)
	// Only requested songs belong to the visible "next"/playlist. The internal
	// library rotation is not a user request and must not appear as a song
	// nobody ordered.
	for _, request := range s.orderedRequestsLocked() {
		for _, track := range s.queue {
			if track.ID == request.TrackID {
				next = entry(track)
				break
			}
		}
		if next == nil {
			next = map[string]any{
				"id": request.TrackID, "title": request.Title,
				"artist": request.Artist, "cover_url": request.CoverURL,
				"lyrics_url": "/api/lyrics/" + request.TrackID,
				"audio": map[string]any{"direct": "/native/" + request.TrackID},
			}
		}
		break
	}
	nextAuto := map[string]any(nil)
	for _, track := range s.queue {
		requested := false
		for _, request := range s.requests {
			if request.TrackID == track.ID && request.Status == "pending" {
				requested = true
				break
			}
		}
		if !requested {
			nextAuto = entry(track)
			break
		}
	}
	direct := ""
	lyricsURL := ""
	if s.current.ID != "" {
		direct = "/native/" + s.current.ID
		lyricsURL = "/api/lyrics/" + s.current.ID
	}
	return map[string]any{
		"seq": s.seq, "audio_rev": s.audioRev, "server_time_ms": now.UnixMilli(),
		"started_at_ms": startedMS, "position_ms": position,
		"duration_ms": s.current.DurationMS, "paused": s.paused,
		"track": entry(s.current), "next": next, "next_auto": nextAuto, "listeners": s.listeners,
		"audio": map[string]any{"direct": direct},
		"lyrics_url": lyricsURL,
		"vote": map[string]any{"votes": len(s.votes), "needed": s.voteNeededLocked(), "voted": false},
	}
}

func (s *State) State(clientKey string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickLocked(time.Now())
	messages := make([]Message, len(s.messages))
	copy(messages, s.messages)
	history := make([]History, len(s.history))
	copy(history, s.history)
	requests := s.orderedRequestsLocked()
	for index := range requests {
		entry := &requests[index]
		entry.Priority = entry.PinCount
		entry.PinnedByMe = false
		for _, client := range entry.PinClients {
			if client == clientKey && clientKey != "" {
				entry.PinnedByMe = true
				break
			}
		}
		entry.CanCancel = entry.ClientKey != "" && entry.ClientKey == clientKey
		// Listener identifiers are only needed for persistence and ownership.
		// Do not expose other listeners' keys in the public state payload.
		entry.PinClients = nil
	}
	return map[string]any{
		"now": s.SnapshotLocked(), "upcoming": requests, "queue": requests,
		"messages": messages, "history": history, "listeners": s.listeners,
	}
}

func (s *State) AddMessage(nickname, body string) Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	message := Message{ID: fmt.Sprintf("m%d", s.nextID), Nickname: nickname, Body: body, CreatedAt: time.Now().UnixMilli()}
	s.messages = append(s.messages, message)
	if len(s.messages) > 200 {
		s.messages = s.messages[len(s.messages)-200:]
	}
	s.saveLocked()
	return message
}

func (s *State) Messages() []Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Message, len(s.messages))
	copy(result, s.messages)
	return result
}

func (s *State) History() []History {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]History, len(s.history))
	copy(result, s.history)
	return result
}

func (s *State) Queue() []QueueEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]QueueEntry, len(s.requests))
	copy(result, s.requests)
	return result
}

func (s *State) Cancel(queueID int64, clientKey string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, entry := range s.requests {
		if entry.QueueID == queueID && (entry.ClientKey == "" || entry.ClientKey == clientKey) {
			trackID := entry.TrackID
			s.requests = append(s.requests[:index], s.requests[index+1:]...)
			for trackIndex, track := range s.queue {
				if track.ID == trackID {
					s.queue = append(s.queue[:trackIndex], s.queue[trackIndex+1:]...)
					break
				}
			}
			s.reorderQueueLocked()
			s.saveLocked()
			return true
		}
	}
	return false
}

func (s *State) CancelAdmin(queueID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, entry := range s.requests {
		if entry.QueueID == queueID {
			trackID := entry.TrackID
			s.requests = append(s.requests[:index], s.requests[index+1:]...)
			for trackIndex, track := range s.queue {
				if track.ID == trackID {
					s.queue = append(s.queue[:trackIndex], s.queue[trackIndex+1:]...)
					break
				}
			}
			s.reorderQueueLocked()
			s.saveLocked()
			return true
		}
	}
	return false
}

func (s *State) orderedRequestsLocked() []QueueEntry {
	result := make([]QueueEntry, 0, len(s.requests))
	for _, entry := range s.requests {
		if entry.Status == "pending" {
			result = append(result, entry)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		left, right := result[i], result[j]
		if left.PinCount != right.PinCount {
			return left.PinCount > right.PinCount
		}
		if left.QueueID != right.QueueID {
			return left.QueueID < right.QueueID
		}
		return left.CreatedAt < right.CreatedAt
	})
	return result
}

// reorderQueueLocked keeps the playable queue in the same order as the
// visible request list. Pins do not mutate request order; they only affect
// the stable sort by unique supporter count.
func (s *State) reorderQueueLocked() {
	ordered := s.orderedRequestsLocked()
	requested := make(map[string]bool, len(ordered))
	for _, entry := range ordered {
		requested[entry.TrackID] = true
	}
	tracks := make(map[string]library.Track, len(s.queue))
	rest := make([]library.Track, 0, len(s.queue))
	for _, track := range s.queue {
		if requested[track.ID] {
			if _, exists := tracks[track.ID]; !exists {
				tracks[track.ID] = track
			}
			continue
		}
		rest = append(rest, track)
	}
	queue := make([]library.Track, 0, len(s.queue))
	for _, entry := range ordered {
		if track, ok := tracks[entry.TrackID]; ok {
			queue = append(queue, track)
		}
	}
	s.queue = append(queue, rest...)
}

func (s *State) TogglePin(queueID int64, clientKey string) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(clientKey) == "" {
		return false, false
	}
	for index := range s.requests {
		entry := &s.requests[index]
		if entry.QueueID != queueID || entry.Status != "pending" {
			continue
		}
		pinned := false
		clients := make([]string, 0, len(entry.PinClients)+1)
		for _, client := range entry.PinClients {
			if client == clientKey {
				pinned = true
				continue
			}
			if client != "" {
				clients = append(clients, client)
			}
		}
		if !pinned {
			clients = append(clients, clientKey)
		}
		entry.PinClients = clients
		entry.PinCount = len(clients)
		entry.Priority = entry.PinCount
		s.reorderQueueLocked()
		s.saveLocked()
		return !pinned, true
	}
	return false, false
}

func (s *State) Vote(client string) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current.ID == "" || s.votes[client] {
		return false, false
	}
	s.votes[client] = true
	if len(s.votes) >= s.voteNeededLocked() {
		s.votes = map[string]bool{}
		s.current = library.Track{}
		s.started = time.Time{}
		s.advanceLocked()
		return true, true
	}
	return true, false
}

func (s *State) VoteSnapshot(client string) map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]any{"votes": len(s.votes), "needed": 3, "voted": s.votes[client]}
}

func (s *State) Settings() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := map[string]any{}
	for key, value := range s.settings {
		result[key] = value
	}
	return result
}

func (s *State) UpdateSettings(values map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range []string{"station_name", "subtitle", "notice", "page_title", "background_interval_sec", "public_url", "lan_url", "tunnel_url", "access_note"} {
		if value, ok := values[key]; ok {
			s.settings[key] = value
			if key == "station_name" {
				s.name = fmt.Sprint(value)
			}
		}
	}
	s.saveLocked()
}

func (s *State) Policy() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := map[string]any{}
	for key, value := range s.policy {
		result[key] = value
	}
	return result
}

func (s *State) UpdatePolicy(values map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, value := range values {
		s.policy[key] = value
	}
	s.saveLocked()
}

func (s *State) ClearQueue() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = nil
	s.saveLocked()
}

func (s *State) DeleteMessage(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, message := range s.messages {
		if message.ID == id {
			s.messages = append(s.messages[:index], s.messages[index+1:]...)
			s.saveLocked()
			return true
		}
	}
	return false
}

func dedupeHistory(items []History) []History {
	seen := map[string]bool{}
	result := make([]History, 0, len(items))
	for _, item := range items {
		if item.TrackID == "" || seen[item.TrackID] {
			continue
		}
		seen[item.TrackID] = true
		result = append(result, item)
	}
	if len(result) > 200 {
		result = result[:200]
	}
	return result
}

func (s *State) repairMetadataLocked() {
	byID := map[string]History{}
	for _, item := range s.history {
		if item.TrackID != "" { byID[item.TrackID] = item }
	}
	fill := func(track *library.Track) {
		if track.ID == "" || (track.Title != "" && track.DurationMS > 0) { return }
		if item, ok := byID[track.ID]; ok {
			if track.Title == "" { track.Title = item.Title }
			if track.Artist == "" { track.Artist = item.Artist }
			if track.CoverURL == "" { track.CoverURL = item.CoverURL }
			if track.DurationMS == 0 { track.DurationMS = item.DurationMS }
			if track.Source == "" { track.Source = item.Source }
			if track.SourceID == "" { track.SourceID = item.SourceID }
			if track.URL == "" { track.URL = item.URL }
		}
	}
	fillFromIndex := func(track *library.Track) {
		if track.ID == "" { return }
		if latest, ok := s.index.Get(track.ID); ok {
			if track.Title == "" { track.Title = latest.Title }
			if track.Artist == "" { track.Artist = latest.Artist }
			if track.Album == "" { track.Album = latest.Album }
			if track.DurationMS == 0 { track.DurationMS = latest.DurationMS }
			if track.CoverURL == "" { track.CoverURL = latest.CoverURL }
			if track.Path == "" { track.Path = latest.Path }
			if track.Source == "" { track.Source = latest.Source }
			if track.SourceID == "" { track.SourceID = latest.SourceID }
		}
	}
	fill(&s.current)
	fillFromIndex(&s.current)
	for index := range s.queue { fill(&s.queue[index]); fillFromIndex(&s.queue[index]) }
	for index := range s.requests {
		entry := &s.requests[index]
		if len(entry.PinClients) == 0 && entry.PinCount > 0 {
			// The old pin implementation only had an anonymous counter and
			// could not be cancelled by an individual listener.
			entry.PinCount = 0
			entry.Priority = 0
		}
		if entry.Title == "" || entry.DurationMS == 0 {
			if item, ok := byID[entry.TrackID]; ok {
				if entry.Title == "" { entry.Title = item.Title }
				if entry.Artist == "" { entry.Artist = item.Artist }
				if entry.CoverURL == "" { entry.CoverURL = item.CoverURL }
				if entry.DurationMS == 0 { entry.DurationMS = item.DurationMS }
			}
		}
	}
}

func (s *State) SetPersistPath(path string) {
	s.mu.Lock()
	s.persistPath = path
	raw, err := os.ReadFile(path)
	if err == nil {
		var payload struct {
			Settings map[string]any  `json:"settings"`
			Policy   map[string]any  `json:"policy"`
			History  []History       `json:"history"`
			Messages []Message       `json:"messages"`
			Requests       []QueueEntry    `json:"requests"`
			Current        *savedTrack      `json:"current"`
			Queue          []savedTrack     `json:"queue"`
			StartedAtMS    int64           `json:"started_at_ms"`
			Paused         bool            `json:"paused"`
			PausedPosition int64           `json:"paused_position_ms"`
			Seq            int             `json:"seq"`
			AudioRev       int             `json:"audio_rev"`
		}
		if json.Unmarshal(raw, &payload) == nil {
			for key, value := range payload.Settings {
				s.settings[key] = value
				if key == "station_name" {
					s.name = fmt.Sprint(value)
				}
			}
			for key, value := range payload.Policy {
				s.policy[key] = value
			}
			if payload.History != nil {
				s.history = dedupeHistory(payload.History)
			}
			if payload.Messages != nil {
				s.messages = payload.Messages
			}
			if payload.Requests != nil {
				s.requests = payload.Requests
			}
			if payload.Current != nil && payload.Current.ID != "" {
				s.current = loadTrack(*payload.Current)
				s.queue = make([]library.Track, 0, len(payload.Queue))
				for _, item := range payload.Queue {
					s.queue = append(s.queue, loadTrack(item))
				}
				s.started = time.UnixMilli(payload.StartedAtMS)
				s.paused = payload.Paused
				s.pausedPosition = payload.PausedPosition
				s.seq = payload.Seq
				s.audioRev = payload.AudioRev
			}
		}
		s.repairMetadataLocked()
		s.saveLocked()
	}
	s.mu.Unlock()
}

func saveTracks(tracks []library.Track) []savedTrack {
	result := make([]savedTrack, 0, len(tracks))
	for _, track := range tracks {
		result = append(result, saveTrack(track))
	}
	return result
}

func (s *State) saveLocked() {
	if s.persistPath == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(s.persistPath), 0o755)
	raw, _ := json.MarshalIndent(map[string]any{
		"settings": s.settings, "policy": s.policy,
		"history": s.history, "messages": s.messages, "requests": s.requests,
		"current": saveTrack(s.current), "queue": saveTracks(s.queue),
		"started_at_ms": s.started.UnixMilli(), "paused": s.paused,
		"paused_position_ms": s.pausedPosition, "seq": s.seq,
		"audio_rev": s.audioRev,
	}, "", "  ")
	_ = os.WriteFile(s.persistPath, raw, 0o600)
}

func (s *State) SetListeners(count int) {
	s.mu.Lock()
	if count < 1 {
		count = 1
	}
	s.listeners = count
	s.mu.Unlock()
}

func (s *State) voteNeededLocked() int {
	needed := s.listeners/2 + 1
	if needed < 1 {
		needed = 1
	}
	return needed
}

func (s *State) Name() string { return s.name }
