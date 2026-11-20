package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"radio-on-stick/station-go/internal/directory"
	"radio-on-stick/station-go/internal/join"
	"radio-on-stick/station-go/internal/library"
	"radio-on-stick/station-go/internal/mesh"
	"radio-on-stick/station-go/internal/plugins"
	"radio-on-stick/station-go/internal/station"
	"radio-on-stick/station-go/internal/stream"
)

type api struct {
	index      *library.Index
	libraryDir []string
	adminToken string
	scanMu     sync.Mutex
	state      *station.State
	source     *stream.Hub
	mesh       *mesh.Hub
	meshSTUN   string
	mediaMode  string
	httpLeafMax int
	httpLeaves  map[string]time.Time
	leafMu      sync.Mutex
	staticDir  string
	uploadsDir string
	directory  *directory.Client
	joinRequired bool
	joinSecret   string
	plugins      *plugins.Client
	backgroundsDir string
	backgroundMu sync.Mutex
	backgroundURLs []string
	backgroundFile string
	libraryFile   string
	pluginMu      sync.Mutex
	pluginCache   map[string]plugins.Candidate
	listenerMu    sync.Mutex
	listeners     map[string]time.Time
	listenerNames map[string]string
	mp3Mu         sync.Mutex
	mp3Cache      map[string]bool
	ownerIPs      map[string]bool
	limitMu       sync.Mutex
	limits        map[string]map[string][]int64
	bannedWords   []string
}

func (a *api) banned(texts ...string) bool {
	if len(a.bannedWords) == 0 {
		return false
	}
	for _, text := range texts {
		value := strings.ToLower(text)
		for _, word := range a.bannedWords {
			if word != "" && strings.Contains(value, strings.ToLower(word)) {
				return true
			}
		}
	}
	return false
}

func (a *api) allow(kind, key string, limit int, window time.Duration) bool {
	if limit <= 0 {
		return true
	}
	now := time.Now().UnixMilli()
	cutoff := now - window.Milliseconds()
	a.limitMu.Lock()
	defer a.limitMu.Unlock()
	bucket := a.limits[kind]
	if bucket == nil {
		bucket = map[string][]int64{}
		a.limits[kind] = bucket
	}
	items := bucket[key]
	kept := items[:0]
	for _, stamp := range items {
		if stamp >= cutoff {
			kept = append(kept, stamp)
		}
	}
	if len(kept) >= limit {
		bucket[key] = kept
		return false
	}
	bucket[key] = append(kept, now)
	return true
}

func (a *api) rateLimited(w http.ResponseWriter, kind string) {
	w.Header().Set("Retry-After", "30")
	writeJSON(w, 429, map[string]any{"error": "rate_limited", "message": kind + " 操作过于频繁，请稍后再试"})
}

func splitWords(raw string) []string {
	result := []string{}
	for _, item := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '，' || r == '\n' }) {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" { return fallback }
	value, err := strconv.Atoi(raw)
	if err != nil { return fallback }
	return value
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func (a *api) admitHTTPLeaf(w http.ResponseWriter, r *http.Request) bool {
	if strings.ToLower(strings.TrimSpace(r.Header.Get("X-Radio-Source-Role"))) != "http" {
		return true
	}
	limit := a.httpLeafMax
	if limit <= 0 {
		return true
	}
	key := a.clientIP(r)
	now := time.Now()
	a.leafMu.Lock()
	for ip, seen := range a.httpLeaves {
		if now.Sub(seen) > 60*time.Second {
			delete(a.httpLeaves, ip)
		}
	}
	_, exists := a.httpLeaves[key]
	count := len(a.httpLeaves)
	if !exists && count >= limit {
		a.leafMu.Unlock()
		w.Header().Set("Retry-After", "10")
		writeJSON(w, 429, map[string]any{"error": "http_leaf_busy", "message": "HTTP 直连叶子已达到上限，请稍后重试"})
		return false
	}
	a.httpLeaves[key] = now
	a.leafMu.Unlock()
	return true
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"ok": true, "service": "radio-station-go",
		"tracks": a.index.Count(), "time": time.Now().UTC().Format(time.RFC3339),
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (a *api) policyEnabled(key string) bool {
	value, ok := a.state.Policy()[key]
	if !ok {
		return true
	}
	enabled, ok := value.(bool)
	return !ok || enabled
}

func (a *api) authorized(r *http.Request) bool {
	if a.ownerIP(r) {
		return true
	}
	if a.adminToken == "" {
		return false
	}
	auth := r.Header.Get("Authorization")
	return strings.HasPrefix(auth, "Bearer ") &&
		strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")) == a.adminToken
}

func (a *api) scan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]any{"error": "method"})
		return
	}
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	if !a.scanMu.TryLock() {
		writeJSON(w, 409, map[string]any{"error": "scan_in_progress"})
		return
	}
	defer a.scanMu.Unlock()
	count, err := a.index.Scan(a.libraryDir, 50000)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": "scan_failed", "message": err.Error()})
		return
	}
	a.state.Rebuild()
	writeJSON(w, 200, map[string]any{"ok": true, "tracks": count})
}

func (a *api) search(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	writeJSON(w, 200, map[string]any{"tracks": a.index.Search(query, limit)})
}

func (a *api) list(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	writeJSON(w, 200, map[string]any{"tracks": a.index.List(limit)})
}

func (a *api) track(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/library/track/")
	track, ok := a.index.Get(id)
	if !ok {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	writeJSON(w, 200, track)
}

func (a *api) stream(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/library/stream/")
	track, ok := a.index.Get(id)
	if !ok || track.Path == "" {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	file, err := os.Open(track.Path)
	if err != nil {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": "stat_failed"})
		return
	}
	http.ServeContent(w, r, filepath.Base(track.Path), info.ModTime(), file)
}

func ownerSet(raw string) map[string]bool {
	result := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result[item] = true
		}
	}
	return result
}

func (a *api) ownerIP(r *http.Request) bool {
	// Only direct/local requests can be owner. A public request through
	// Caddy/Relay carries X-Forwarded-For and must never inherit admin rights.
	if r.Header.Get("X-Forwarded-For") != "" || r.Header.Get("X-Real-IP") != "" {
		return false
	}
	ip := a.clientIP(r)
	return a.ownerIPs[ip]
}

func (a *api) ownerRequest(r *http.Request) bool {
	return r.URL.Path == "/" && a.ownerIP(r)
}

func (a *api) adminOwner(w http.ResponseWriter, r *http.Request) {
	if !a.ownerIP(r) {
		writeJSON(w, 403, map[string]any{"error": "not_owner"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "owner": true})
}

func (a *api) clientIP(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		if first := strings.TrimSpace(strings.Split(forwarded, ",")[0]); first != "" {
			return first
		}
	}
	if real := strings.TrimSpace(r.Header.Get("X-Real-IP")); real != "" {
		return real
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (a *api) touchListener(w http.ResponseWriter, r *http.Request, nickname string) string {
	now := time.Now()
	key := ""
	if cookie, err := r.Cookie("radio_lid"); err == nil && len(cookie.Value) >= 16 {
		key = cookie.Value
	}
	if key == "" {
		random := make([]byte, 16)
		_, _ = rand.Read(random)
		key = hex.EncodeToString(random)
		http.SetCookie(w, &http.Cookie{
			Name: "radio_lid", Value: key, Path: "/", HttpOnly: true,
			SameSite: http.SameSiteLaxMode, MaxAge: 30 * 24 * 60 * 60,
			Secure: r.Header.Get("X-Forwarded-Proto") == "https",
		})
	}
	a.listenerMu.Lock()
	a.listeners[key] = now
	if nickname != "" {
		a.listenerNames[key] = nickname
	}
	for client, seen := range a.listeners {
		if now.Sub(seen) > 35*time.Second {
			delete(a.listeners, client)
			delete(a.listenerNames, client)
		}
	}
	count := len(a.listeners)
	a.listenerMu.Unlock()
	if count < 1 {
		count = 1
	}
	a.state.SetListeners(count)
	return key
}

func (a *api) listenersAPI(w http.ResponseWriter, r *http.Request) {
	nickname := strings.TrimSpace(r.URL.Query().Get("nickname"))
	a.touchListener(w, r, nickname)
	now := time.Now()
	a.listenerMu.Lock()
	items := make([]map[string]any, 0, len(a.listeners))
	for client, seen := range a.listeners {
		if now.Sub(seen) > 35*time.Second {
			delete(a.listeners, client)
			delete(a.listenerNames, client)
			continue
		}
		name := a.listenerNames[client]
		if name == "" {
			name = "听众"
		}
		items = append(items, map[string]any{"nickname": name, "last_seen": seen.UnixMilli()})
	}
	count := len(a.listeners)
	a.listenerMu.Unlock()
	writeJSON(w, 200, map[string]any{"ok": true, "listeners": count, "items": items})
}

func (a *api) now(w http.ResponseWriter, r *http.Request) {
	a.touchListener(w, r, "")
	snapshot := a.state.Snapshot()
	snapshot["mesh"] = map[string]any{"enabled": true, "ws_url": "/ws", "stun": a.meshSTUN}
	writeJSON(w, 200, snapshot)
}

func (a *api) stateJSON(w http.ResponseWriter, r *http.Request) {
	state := a.state.State(a.touchListener(w, r, ""))
	if now, ok := state["now"].(map[string]any); ok {
		now["mesh"] = map[string]any{"enabled": true, "ws_url": "/ws", "stun": a.meshSTUN}
	}
	writeJSON(w, 200, state)
}

func (a *api) saveBackgroundURLsLocked() {
	raw, _ := json.MarshalIndent(a.backgroundURLs, "", "  ")
	_ = os.MkdirAll(filepath.Dir(a.backgroundFile), 0o755)
	_ = os.WriteFile(a.backgroundFile, raw, 0o600)
}

func (a *api) backgroundItems() []map[string]any {
	items := []map[string]any{}
	entries, _ := os.ReadDir(a.backgroundsDir)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".jpg" && ext != ".jpeg" && ext != ".png" && ext != ".webp" {
			continue
		}
		items = append(items, map[string]any{"name": name, "url": "/backgrounds/" + name, "type": "local"})
	}
	a.backgroundMu.Lock()
	for _, url := range a.backgroundURLs {
		items = append(items, map[string]any{"name": url, "url": url, "type": "network"})
	}
	a.backgroundMu.Unlock()
	return items
}

func (a *api) config(w http.ResponseWriter, r *http.Request) {
	settings := a.state.Settings()
	settings["backgrounds"] = a.backgroundItems()
	settings["media_mode"] = a.mediaMode
	writeJSON(w, 200, settings)
}

func (a *api) requestTrack(trackID string) (library.Track, bool) {
	history, hasHistory := a.state.HistoryByTrackID(trackID)
	fillHistory := func(track *library.Track) {
		if !hasHistory {
			return
		}
		if track.Title == "" { track.Title = history.Title }
		if track.Artist == "" { track.Artist = history.Artist }
		if track.CoverURL == "" { track.CoverURL = history.CoverURL }
		if track.DurationMS == 0 { track.DurationMS = history.DurationMS }
	}
	if source, sourceID, found := strings.Cut(trackID, ":"); found {
		a.pluginMu.Lock()
		item, cached := a.pluginCache[trackID]
		a.pluginMu.Unlock()
		// Bilibili CDN URLs are short-lived. Keep the identity now and resolve
		// the audio only when native/source playback is about to start.
		if strings.EqualFold(source, "bilibili") {
			if item.Title == "" {
				// Old imported history has only the BVID. Resolve metadata now
				// but deliberately discard the short-lived audio URL so native/
				// source can refresh it when playback is actually near.
				if resolved, err := a.plugins.Resolve(source, sourceID); err == nil {
					item = resolved
				}
			}
			track := library.Track{
				ID: trackID, Title: item.Title, Artist: item.Artist,
				Album: item.Album, DurationMS: item.DurationMS,
				Source: "bilibili", SourceID: sourceID, CoverURL: item.CoverURL,
			}
			fillHistory(&track)
			return track, true
		}
		resolved := item
		if !cached || resolved.AudioURL == "" {
			var err error
			resolved, err = a.plugins.Resolve(source, sourceID)
			if err != nil || resolved.AudioURL == "" {
				return library.Track{}, false
			}
		}
		if cached {
			if resolved.Title == "" { resolved.Title = item.Title }
			if resolved.Artist == "" { resolved.Artist = item.Artist }
			if resolved.Album == "" { resolved.Album = item.Album }
			if resolved.DurationMS == 0 { resolved.DurationMS = item.DurationMS }
			if resolved.CoverURL == "" { resolved.CoverURL = item.CoverURL }
		}
		track := library.Track{
			ID: trackID, Title: resolved.Title, Artist: resolved.Artist,
			Album: resolved.Album, DurationMS: resolved.DurationMS,
			URL: resolved.AudioURL, Source: resolved.Source, SourceID: resolved.SourceID,
			CoverURL: resolved.CoverURL,
		}
		fillHistory(&track)
		return track, true
	}
	if local, found := a.index.Get(trackID); found {
		return local, true
	}
	if queued, found := a.state.TrackByID(trackID); found {
		return queued, true
	}
	history, found := a.state.HistoryByTrackID(trackID)
	if !found {
		return library.Track{}, false
	}
	return a.trackFromHistory(history)
}

func (a *api) trackFromHistory(history station.History) (library.Track, bool) {
	track := library.Track{
		ID: history.TrackID, Title: history.Title, Artist: history.Artist,
		DurationMS: history.DurationMS, Source: history.Source,
		SourceID: history.SourceID, URL: history.URL, CoverURL: history.CoverURL,
	}
	if track.Source != "" && !strings.EqualFold(track.Source, "bilibili") && track.SourceID != "" {
		resolved, err := a.plugins.Resolve(track.Source, track.SourceID)
		if err == nil && resolved.AudioURL != "" {
			track.URL = resolved.AudioURL
			if track.DurationMS == 0 { track.DurationMS = resolved.DurationMS }
			if track.CoverURL == "" { track.CoverURL = resolved.CoverURL }
		}
	}
	if track.Source == "" {
		// Old imported history predates source metadata. Resolve by title/artist.
		query := strings.TrimSpace(track.Title + " " + track.Artist)
		if query != "" {
			for _, item := range a.plugins.Search(query) {
				if item.Source == "" || item.SourceID == "" { continue }
				track.Source = item.Source
				track.SourceID = item.SourceID
				track.DurationMS = item.DurationMS
				track.CoverURL = item.CoverURL
				if !strings.EqualFold(item.Source, "bilibili") {
					track.URL = item.AudioURL
				}
				break
			}
		}
	}
	if track.ID == "" {
		return library.Track{}, false
	}
	if track.URL == "" && track.Source != "bilibili" && track.Source == "" {
		return library.Track{}, false
	}
	return track, true
}

func (a *api) ensureTrackAudio(track *library.Track) bool {
	if track == nil || track.ID == "" { return false }
	// Local library and uploads are files, not remote URLs; they are served by
	// /native and /source directly from Path.
	if track.Path != "" {
		if latest, ok := a.index.Get(track.ID); ok {
			if track.CoverURL == "" { track.CoverURL = latest.CoverURL }
			if track.Title == "" { track.Title = latest.Title }
			if track.Artist == "" { track.Artist = latest.Artist }
			if track.Album == "" { track.Album = latest.Album }
			if track.DurationMS == 0 { track.DurationMS = latest.DurationMS }
			a.state.UpdateTrackMetadata(*track)
		}
		return true
	}
	needsMetadata := track.Title == "" || track.Album == ""
	if track.URL != "" && !needsMetadata { return true }
	if track.Source == "" || track.SourceID == "" { return false }
	resolved, err := a.plugins.Resolve(track.Source, track.SourceID)
	if err != nil || resolved.AudioURL == "" { return false }
	if track.URL == "" { track.URL = resolved.AudioURL }
	if track.Title == "" { track.Title = resolved.Title }
	if track.Artist == "" { track.Artist = resolved.Artist }
	if track.Album == "" { track.Album = resolved.Album }
	if track.DurationMS == 0 { track.DurationMS = resolved.DurationMS }
	if track.CoverURL == "" { track.CoverURL = resolved.CoverURL }
	a.state.SetTrackAudio(track.ID, track.URL)
	a.state.UpdateTrackMetadata(*track)
	return true
}

func (a *api) request(w http.ResponseWriter, r *http.Request) {
	if !a.allow("request", a.clientIP(r), 30, time.Minute) {
		a.rateLimited(w, "点歌")
		return
	}
	if !a.policyEnabled("requests_enabled") {
		writeJSON(w, 403, map[string]any{"error": "requests_disabled"})
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]any{"error": "method"})
		return
	}
	var body struct {
		TrackID     string `json:"track_id"`
		CandidateID string `json:"candidate_id"`
		Nickname    string `json:"nickname"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	trackID := strings.TrimSpace(body.TrackID)
	if trackID == "" {
		trackID = strings.TrimSpace(body.CandidateID)
	}
	if trackID == "" {
		writeJSON(w, 400, map[string]any{"error": "missing_track_id"})
		return
	}
	track, ok := a.requestTrack(trackID)
	if !ok {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	if a.banned(body.Nickname) {
		writeJSON(w, 400, map[string]any{"error": "blocked_word"})
		return
	}
	a.state.Request(track, strings.TrimSpace(body.Nickname), a.touchListener(w, r, strings.TrimSpace(body.Nickname)))
	writeJSON(w, 200, map[string]any{"ok": true, "track": track})
}

func (a *api) skip(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	a.state.Skip()
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) pause(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	paused := true
	var body struct {
		Paused *bool `json:"paused"`
	}
	if r.ContentLength > 0 && json.NewDecoder(r.Body).Decode(&body) == nil && body.Paused != nil {
		paused = *body.Paused
	}
	a.state.Pause(paused)
	writeJSON(w, 200, map[string]any{"ok": true, "paused": paused})
}

func (a *api) resume(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	a.state.Pause(false)
	writeJSON(w, 200, map[string]any{"ok": true, "paused": false})
}

func (a *api) meshStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, a.mesh.Status())
}

func (a *api) mediaUpload(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/media/uploads/")
	if name == "" || strings.ContainsAny(name, "/\\") || name == "." || name == ".." {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	http.ServeFile(w, r, filepath.Join(a.uploadsDir, name))
}

func (a *api) sourceManifest(w http.ResponseWriter, r *http.Request) {
	if !a.admitHTTPLeaf(w, r) {
		return
	}
	track := a.state.Current()
	if track.URL == "" && !a.ensureTrackAudio(&track) {
		writeJSON(w, 502, map[string]any{"error": "audio_unavailable", "message": "音源解析失败"})
		return
	}
	if mp3Only, ok := a.state.Policy()["mp3_remote_only"].(bool); ok && mp3Only && track.URL != "" && !a.remoteIsMP3(track.URL, track.Source) {
		writeJSON(w, 415, map[string]any{"error": "mp3_only", "message": "管理员只允许 MP3 第三方音源"})
		return
	}
	manifest, err := a.source.Manifest(track, a.state.Revision(), a.state.PositionMS())
	if err != nil {
		writeJSON(w, 409, map[string]any{"error": "no_stream"})
		return
	}
	writeJSON(w, 200, manifest)
}

func (a *api) sourceChunk(w http.ResponseWriter, r *http.Request) {
	if !a.admitHTTPLeaf(w, r) {
		return
	}
	streamID, _ := strconv.ParseUint(r.URL.Query().Get("stream_id"), 10, 32)
	seq, _ := strconv.ParseUint(r.URL.Query().Get("seq"), 10, 32)
	frame, err := a.source.Chunk(uint32(streamID), uint32(seq))
	if err != nil {
		writeJSON(w, 404, map[string]any{"error": "chunk_unavailable"})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(frame)))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(200)
	_, _ = w.Write(frame)
}

func (a *api) remoteIsMP3(rawURL, source string) bool {
	a.mp3Mu.Lock()
	if known, ok := a.mp3Cache[rawURL]; ok {
		a.mp3Mu.Unlock()
		return known
	}
	a.mp3Mu.Unlock()
	client := &http.Client{Timeout: 8 * time.Second}
	request, err := http.NewRequest(http.MethodHead, rawURL, nil)
	if err != nil {
		return false
	}
	for key, value := range stream.UpstreamHeaders(library.Track{Source: source}) {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	ok := false
	if err == nil {
		defer response.Body.Close()
		contentType := strings.ToLower(response.Header.Get("Content-Type"))
		ok = response.StatusCode >= 200 && response.StatusCode < 400 &&
			(strings.Contains(contentType, "audio/mpeg") || strings.Contains(contentType, "audio/mp3"))
	}
	a.mp3Mu.Lock()
	a.mp3Cache[rawURL] = ok
	a.mp3Mu.Unlock()
	return ok
}

func (a *api) proxyRemoteAudio(w http.ResponseWriter, r *http.Request, rawURL, source string) {
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, rawURL, nil)
	if err != nil {
		http.Error(w, "audio unavailable", http.StatusBadGateway)
		return
	}
	if value := r.Header.Get("Range"); value != "" {
		request.Header.Set("Range", value)
	}
	for key, value := range stream.UpstreamHeaders(library.Track{Source: source}) {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		http.Error(w, "audio unavailable", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	for _, header := range []string{"Content-Type", "Content-Length", "Accept-Ranges", "Content-Range"} {
		if value := response.Header.Get(header); value != "" {
			w.Header().Set(header, value)
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(response.StatusCode)
	buffer := make([]byte, 32*1024)
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			if _, writeErr := w.Write(buffer[:n]); writeErr != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if readErr != nil {
			return
		}
	}
}

func (a *api) coverProxy(w http.ResponseWriter, r *http.Request) {
	raw := strings.TrimSpace(r.URL.Query().Get("url"))
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_cover"})
		return
	}
	host := strings.ToLower(parsed.Hostname())
	allowed := host == "bilibili.com" || strings.HasSuffix(host, ".bilibili.com") ||
		host == "hdslb.com" || strings.HasSuffix(host, ".hdslb.com")
	if !allowed {
		writeJSON(w, 400, map[string]any{"error": "bad_cover_host"})
		return
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, parsed.String(), nil)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_cover"})
		return
	}
	request.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36")
	request.Header.Set("Referer", "https://www.bilibili.com/")
	client := &http.Client{Timeout: 12 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		writeJSON(w, 502, map[string]any{"error": "cover_upstream"})
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		writeJSON(w, 502, map[string]any{"error": "cover_upstream"})
		return
	}
	w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(response.Body, 8*1024*1024))
}

func (a *api) native(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/native/")
	track, ok := a.index.Get(id)
	if !ok {
		// Remote tracks are not in the local index. The active track and the
		// already-resolved next track must both be servable: the browser starts
		// preloading the next native URL roughly 45 seconds before the switch.
		if queued, exists := a.state.TrackByID(id); exists {
			track, ok = queued, true
		} else if current := a.state.Current(); current.ID == id {
			track, ok = current, true
		}
		if !ok {
			writeJSON(w, 404, map[string]any{"error": "not_found"})
			return
		}
	}
	if track.URL == "" {
		if !a.ensureTrackAudio(&track) {
			writeJSON(w, 502, map[string]any{"error": "audio_unavailable", "message": "音源解析失败"})
			return
		}
	}
	if track.URL != "" {
		if a.remoteIsMP3(track.URL, track.Source) {
			a.proxyRemoteAudio(w, r, track.URL, track.Source)
			return
		}
		args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-re"}
		if a.state.Current().ID == track.ID {
			if position := a.state.PositionMS(); position > 0 {
				args = append(args, "-ss", fmt.Sprintf("%.3f", float64(position)/1000))
			}
		}
		if headers := stream.UpstreamHeaders(track); headers != nil {
			args = append(args, "-headers", fmt.Sprintf("Referer: %s\r\nUser-Agent: %s\r\n", headers["Referer"], headers["User-Agent"]))
		}
		args = append(args, "-i", track.URL, "-vn", "-c:a", "libmp3lame", "-b:a", "128k", "-f", "mp3", "pipe:1")
		if mp3Only, ok := a.state.Policy()["mp3_remote_only"].(bool); ok && mp3Only {
			writeJSON(w, 415, map[string]any{"error": "mp3_only", "message": "管理员只允许 MP3 第三方音源"})
			return
		}
		cmd := exec.CommandContext(r.Context(), "ffmpeg", args...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			http.Error(w, "audio unavailable", http.StatusBadGateway)
			return
		}
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			http.Error(w, "audio unavailable", http.StatusBadGateway)
			return
		}
		defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
		w.Header().Set("Content-Type", "audio/mpeg")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		buffer := make([]byte, 32*1024)
		for {
			n, err := stdout.Read(buffer)
			if n > 0 {
				if _, writeErr := w.Write(buffer[:n]); writeErr != nil {
					return
				}
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			}
			if err != nil {
				return
			}
		}
	}
	if track.Path == "" {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	file, err := os.Open(track.Path)
	if err != nil {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": "stat_failed"})
		return
	}
	http.ServeContent(w, r, filepath.Base(track.Path), info.ModTime(), file)
}

func (a *api) joinAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 200, map[string]any{"ok": true, "join_required": a.joinRequired})
		return
	}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	if a.directory == nil || !a.directory.Verify(body.Code) {
		writeJSON(w, 403, map[string]any{"error": "invalid_code", "message": "邀请码无效"})
		return
	}
	token := join.Make(a.joinSecret, body.Code, 7*24*time.Hour)
	cookie := &http.Cookie{
		Name: join.CookieName, Value: token, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: 7 * 24 * 60 * 60,
		Secure: r.Header.Get("X-Forwarded-Proto") == "https",
	}
	http.SetCookie(w, cookie)
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) validJoin(r *http.Request) bool {
	cookie, err := r.Cookie(join.CookieName)
	if err != nil || a.directory == nil {
		return false
	}
	return join.Verify(a.joinSecret, cookie.Value, a.directory.InviteCode())
}

func (a *api) listenerLimitReached(r *http.Request) bool {
	raw, ok := a.state.Policy()["max_listeners"]
	if !ok {
		return false
	}
	limit := 0
	switch value := raw.(type) {
	case float64:
		limit = int(value)
	case int:
		limit = value
	case int64:
		limit = int(value)
	}
	if limit <= 0 {
		return false
	}
	key := ""
	if cookie, err := r.Cookie("radio_lid"); err == nil {
		key = cookie.Value
	}
	now := time.Now()
	a.listenerMu.Lock()
	defer a.listenerMu.Unlock()
	if key != "" {
		if seen, exists := a.listeners[key]; exists && now.Sub(seen) <= 35*time.Second {
			return false
		}
	}
	active := 0
	for _, seen := range a.listeners {
		if now.Sub(seen) <= 35*time.Second {
			active++
		}
	}
	return active >= limit
}

func (a *api) secure(next http.Handler) http.Handler {
	public := map[string]bool{
		"/join": true, "/join/": true,
		"/join.html": true, "/join.js": true, "/app.js": true,
		"/admin.js": true, "/mesh.js": true, "/style.css": true,
		"/admin.css": true, "/placeholder.svg": true, "/manifest.webmanifest": true,
		"/sw.js": true, "/favicon.ico": true, "/healthz": true, "/api/config": true,
		"/api/join": true, "/admin": true, "/admin/": true,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.ownerRequest(r) {
			http.Redirect(w, r, "/admin", http.StatusFound)
			return
		}
		// Direct server access is the owner, so the local player/queue/listener
		// APIs must work without an invite cookie. Proxied requests carry
		// X-Forwarded-For and are deliberately excluded above.
		if a.ownerIP(r) {
			next.ServeHTTP(w, r)
			return
		}
		if a.listenerLimitReached(r) && (r.URL.Path == "/" || r.URL.Path == "/index.html" || r.URL.Path == "/now.json" || r.URL.Path == "/api/state" || r.URL.Path == "/ws") {
			w.Header().Set("Retry-After", "15")
			writeJSON(w, 503, map[string]any{"error": "access_full", "message": "当前听众人数已满，请稍后再试"})
			return
		}
		if !a.joinRequired || public[r.URL.Path] || strings.HasPrefix(r.URL.Path, "/api/admin/") || strings.HasPrefix(r.URL.Path, "/backgrounds/") || a.validJoin(r) {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/join", http.StatusFound)
			return
		}
		writeJSON(w, 401, map[string]any{"error": "join_required", "message": "请输入邀请码加入电台"})
	})
}

func (a *api) adminBackground(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	mime := strings.ToLower(strings.Split(r.Header.Get("Content-Type"), ";")[0])
	ext := map[string]string{"image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png", "image/webp": ".webp"}[mime]
	if ext == "" {
		writeJSON(w, 400, map[string]any{"error": "bad_image"})
		return
	}
	random := make([]byte, 16)
	_, _ = rand.Read(random)
	name := hex.EncodeToString(random) + ext
	file, err := os.Create(filepath.Join(a.backgroundsDir, name))
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": "open_failed"})
		return
	}
	n, err := io.Copy(file, io.LimitReader(r.Body, 16*1024*1024))
	_ = file.Close()
	if err != nil || n == 0 {
		_ = os.Remove(filepath.Join(a.backgroundsDir, name))
		writeJSON(w, 400, map[string]any{"error": "upload_failed"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "name": name, "url": "/backgrounds/" + name})
}

func (a *api) adminBackgroundDelete(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	var body struct{ Name string `json:"name"` }
	if json.NewDecoder(r.Body).Decode(&body) != nil || strings.ContainsAny(body.Name, "/\\") {
		writeJSON(w, 400, map[string]any{"error": "bad_name"})
		return
	}
	_ = os.Remove(filepath.Join(a.backgroundsDir, body.Name))
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) adminBackgroundURL(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	var body struct{ URL string `json:"url"` }
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	parsed, err := url.Parse(body.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		writeJSON(w, 400, map[string]any{"error": "bad_url"})
		return
	}
	if r.URL.Path == "/api/admin/background/url/delete" {
		a.backgroundMu.Lock()
		for i, item := range a.backgroundURLs {
			if item == body.URL {
				a.backgroundURLs = append(a.backgroundURLs[:i], a.backgroundURLs[i+1:]...)
				a.saveBackgroundURLsLocked()
				break
			}
		}
		a.backgroundMu.Unlock()
	} else {
		a.backgroundMu.Lock()
		a.backgroundURLs = append(a.backgroundURLs, body.URL)
		a.saveBackgroundURLsLocked()
		a.backgroundMu.Unlock()
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) backgrounds(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/backgrounds/")
	if strings.ContainsAny(name, "/\\") || name == "" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, filepath.Join(a.backgroundsDir, name))
}

func (a *api) adminDirectory(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	if a.directory == nil {
		writeJSON(w, 200, map[string]any{"ok": true, "directory": map[string]any{"enabled": false}})
		return
	}
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/refresh") {
		_ = a.directory.Refresh()
	} else if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/rotate") {
		a.directory.Rotate()
	} else if r.Method == http.MethodPost {
		var body struct {
			Code string `json:"code"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if strings.TrimSpace(body.Code) != "" {
			if err := a.directory.SetInvite(body.Code); err != nil {
				status := 400
				message := err.Error()
				if strings.Contains(message, "409") {
					status = 409
					message = "邀请码已被其他电台使用"
				}
				writeJSON(w, status, map[string]any{"error": "invalid_invite", "message": message})
				return
			}
		} else {
			a.directory.Rotate()
		}
	} else {
		_ = a.directory.Refresh()
	}
	status := a.directory.Status(true)
	writeJSON(w, 200, map[string]any{"ok": true, "directory": status})
}

func (a *api) root(w http.ResponseWriter, r *http.Request) {
	if a.ownerRequest(r) {
		http.Redirect(w, r, "/admin", http.StatusFound)
		return
	}
	if r.URL.Path == "/admin" || r.URL.Path == "/admin/" {
		http.ServeFile(w, r, filepath.Join(a.staticDir, "index.html"))
		return
	}
	if r.URL.Path == "/join" || r.URL.Path == "/join/" {
		http.ServeFile(w, r, filepath.Join(a.staticDir, "join.html"))
		return
	}
	http.FileServer(http.Dir(a.staticDir)).ServeHTTP(w, r)
}

func (a *api) queueCancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		QueueID int64 `json:"queue_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	if !a.state.Cancel(body.QueueID, a.touchListener(w, r, "")) {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) queuePin(w http.ResponseWriter, r *http.Request) {
	if !a.allow("pin", a.clientIP(r), 10, time.Minute) {
		a.rateLimited(w, "置顶")
		return
	}
	var body struct {
		QueueID int64 `json:"queue_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	clientKey := a.touchListener(w, r, "")
	pinned, ok := a.state.TogglePin(body.QueueID, clientKey)
	if !ok {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "pinned": pinned})
}

func (a *api) upload(w http.ResponseWriter, r *http.Request) {
	if !a.allow("upload", a.clientIP(r), 5, time.Minute) {
		a.rateLimited(w, "上传")
		return
	}
	if !a.policyEnabled("uploads_enabled") {
		writeJSON(w, 403, map[string]any{"error": "uploads_disabled"})
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]any{"error": "method"})
		return
	}
	filename := r.Header.Get("X-Filename")
	if decoded, err := url.QueryUnescape(filename); err == nil {
		filename = decoded
	}
	ext := strings.ToLower(filepath.Ext(filename))
	allowed := map[string]bool{".mp3": true, ".aac": true, ".m4a": true, ".ogg": true, ".oga": true, ".opus": true, ".wav": true, ".flac": true, ".webm": true}
	if mp3Only, ok := a.state.Policy()["mp3_upload_only"].(bool); ok && mp3Only && ext != ".mp3" {
		writeJSON(w, 415, map[string]any{"error": "mp3_only"})
		return
	}
	if !allowed[ext] {
		writeJSON(w, 400, map[string]any{"error": "bad_extension"})
		return
	}
	base := fmt.Sprintf("upload-%d%s", time.Now().UnixNano(), ext)
	path := filepath.Join(a.uploadsDir, base)
	file, err := os.Create(path)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": "open_failed"})
		return
	}
	limited := io.LimitReader(r.Body, 64*1024*1024)
	n, err := io.Copy(file, limited)
	_ = file.Close()
	if err != nil || n == 0 {
		_ = os.Remove(path)
		writeJSON(w, 400, map[string]any{"error": "upload_failed"})
		return
	}
	if _, err := a.index.Scan(append(append([]string{}, a.libraryDir...), a.uploadsDir), 50000); err != nil {
		writeJSON(w, 500, map[string]any{"error": "scan_failed"})
		return
	}
	a.state.Rebuild()
	writeJSON(w, 200, map[string]any{"ok": true, "name": base})
}

func (a *api) messages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, map[string]any{"messages": a.state.Messages()})
	case http.MethodPost:
		if !a.allow("message", a.clientIP(r), 10, time.Minute) {
			a.rateLimited(w, "留言")
			return
		}
		if !a.policyEnabled("messages_enabled") {
			writeJSON(w, 403, map[string]any{"error": "messages_disabled"})
			return
		}
		var body struct {
			Nickname string `json:"nickname"`
			Body     string `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]any{"error": "bad_json"})
			return
		}
		text := strings.TrimSpace(body.Body)
		if a.banned(body.Nickname, text) {
			writeJSON(w, 400, map[string]any{"error": "blocked_word"})
			return
		}
		if text == "" || len([]rune(text)) > 200 {
			writeJSON(w, 400, map[string]any{"error": "bad_message"})
			return
		}
		message := a.state.AddMessage(strings.TrimSpace(body.Nickname), text)
		writeJSON(w, 200, map[string]any{"ok": true, "message": message})
	default:
		writeJSON(w, 405, map[string]any{"error": "method"})
	}
}

func (a *api) voteSkip(w http.ResponseWriter, r *http.Request) {
	if !a.allow("vote", a.clientIP(r), 10, time.Minute) {
		a.rateLimited(w, "投票")
		return
	}
	host := a.touchListener(w, r, r.URL.Query().Get("nickname"))
	voted, switched := a.state.Vote(host)
	now := a.state.Snapshot()
	if vote, ok := now["vote"].(map[string]any); ok {
		vote["voted"] = voted
	}
	writeJSON(w, 200, map[string]any{"ok": true, "now": now, "switched": switched})
}

func (a *api) report(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, a.state.Snapshot())
}

func normalizeLocalLRC(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "{") {
			var payload struct {
				T int64 `json:"t"`
				C []struct {
					Tx string `json:"tx"`
				} `json:"c"`
			}
			if json.Unmarshal([]byte(trimmed), &payload) == nil && len(payload.C) > 0 {
				var lyric strings.Builder
				for _, part := range payload.C {
					lyric.WriteString(part.Tx)
				}
				ms := payload.T
				if ms < 0 { ms = 0 }
				minutes := ms / 60000
				seconds := (ms % 60000) / 1000
				millis := ms % 1000
				fmt.Fprintf(&out, "[%02d:%02d.%03d]%s\n", minutes, seconds, millis, lyric.String())
				continue
			}
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	return strings.TrimSpace(out.String())
}

func localLyricsFor(path string) string {
	if path == "" {
		return ""
	}
	base := strings.TrimSuffix(path, filepath.Ext(path))
	for _, ext := range []string{".lrc", ".LRC"} {
		raw, err := os.ReadFile(base + ext)
		if err != nil || len(raw) == 0 {
			continue
		}
		text := strings.TrimPrefix(string(raw), "\ufeff")
		text = normalizeLocalLRC(text)
		if strings.TrimSpace(text) != "" {
			return text
		}
	}
	return ""
}

func (a *api) lyrics(w http.ResponseWriter, r *http.Request) {
	trackID := strings.TrimPrefix(r.URL.Path, "/api/lyrics/")
	source, sourceID, found := strings.Cut(trackID, ":")
	localPath := ""
	if track, ok := a.state.TrackByID(trackID); ok {
		if !found {
			source, sourceID = track.Source, track.SourceID
		}
		localPath = track.Path
	} else if track, ok := a.index.Get(trackID); ok {
		if !found {
			source, sourceID = track.Source, track.SourceID
		}
		localPath = track.Path
	}
	lrc, translation := "", ""
	if localPath != "" {
		lrc = localLyricsFor(localPath)
	}
	if lrc == "" && (found || (source != "" && sourceID != "")) && !strings.EqualFold(source, "local") && !strings.EqualFold(source, "upload") {
		if a.plugins != nil {
			lrc, translation = a.plugins.Lyrics(source, sourceID)
		}
	}
	writeJSON(w, 200, map[string]any{"track_id": trackID, "lrc": lrc, "translation": translation})
}

// localCover serves a sidecar image or the embedded album-art stream without
// exposing arbitrary filesystem paths. Only IDs already present in the index
// are accepted.
func (a *api) localCover(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/local-cover/")
	track, ok := a.index.Get(id)
	if !ok || track.Path == "" {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	base := strings.TrimSuffix(track.Path, filepath.Ext(track.Path))
	candidates := []string{}
	for _, ext := range []string{".jpg", ".jpeg", ".png", ".webp"} {
		candidates = append(candidates,
			base+ext,
			filepath.Join(filepath.Dir(track.Path), "cover"+ext),
			filepath.Join(filepath.Dir(track.Path), "folder"+ext),
			filepath.Join(filepath.Dir(track.Path), "front"+ext),
		)
	}
	for _, candidate := range candidates {
		raw, err := os.ReadFile(candidate)
		if err != nil || len(raw) == 0 || len(raw) > 16*1024*1024 {
			continue
		}
		contentType := http.DetectContentType(raw)
		if !strings.HasPrefix(contentType, "image/") {
			continue
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
		_, _ = w.Write(raw)
		return
	}
	// Fall back to an embedded cover. FFmpeg writes the first image stream to
	// stdout; this avoids creating a permanent cache directory.
	cmd := exec.CommandContext(r.Context(), "ffmpeg", "-v", "error", "-i", track.Path,
		"-map", "0:v:0", "-frames:v", "1", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1")
	raw, err := cmd.Output()
	if err != nil || len(raw) == 0 {
		writeJSON(w, 404, map[string]any{"error": "no_cover"})
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	_, _ = w.Write(raw)
}

func (a *api) adminLibrary(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, 200, map[string]any{"ok": true, "dirs": a.libraryDir, "tracks": a.index.Count()})
		return
	}
	var body struct {
		Dirs []string `json:"dirs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	dirs := make([]string, 0, len(body.Dirs))
	for _, item := range body.Dirs {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if info, err := os.Stat(item); err != nil || !info.IsDir() {
			writeJSON(w, 400, map[string]any{"error": "bad_directory", "path": item})
			return
		}
		dirs = append(dirs, item)
	}
	a.libraryDir = dirs
	raw, _ := json.MarshalIndent(dirs, "", "  ")
	_ = os.MkdirAll(filepath.Dir(a.libraryFile), 0o755)
	_ = os.WriteFile(a.libraryFile, raw, 0o600)
	count, err := a.index.Scan(append(append([]string{}, dirs...), a.uploadsDir), 50000)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": "scan_failed", "message": err.Error()})
		return
	}
	a.state.Rebuild()
	writeJSON(w, 200, map[string]any{"ok": true, "dirs": dirs, "tracks": count})
}

func (a *api) adminLibraryScan(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]any{"error": "method"})
		return
	}
	count, err := a.index.Scan(append(append([]string{}, a.libraryDir...), a.uploadsDir), 50000)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": "scan_failed", "message": err.Error()})
		return
	}
	a.state.Rebuild()
	writeJSON(w, 200, map[string]any{"ok": true, "tracks": count})
}

func (a *api) adminState(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	state := a.state.State("")
	result := map[string]any{
		"now": state["now"], "upcoming": state["upcoming"], "queue": state["queue"],
		"messages": state["messages"], "history": state["history"],
		"policy": a.state.Policy(), "settings": a.state.Settings(),
		"tracks": a.index.List(200), "backgrounds": a.backgroundItems(),
		"library": map[string]any{"dirs": a.libraryDir, "tracks": a.index.Count()},
		"directory": func() any { if a.directory == nil { return map[string]any{"enabled": false} }; return a.directory.Status(true) }(),
	}
	writeJSON(w, 200, result)
}

func (a *api) adminClearQueue(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	a.state.ClearQueue()
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) adminQueueDelete(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	var body struct {
		QueueID int64 `json:"queue_id"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	a.state.CancelAdmin(body.QueueID)
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) adminTrack(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	var body struct {
		CandidateID string `json:"candidate_id"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	if source, sourceID, found := strings.Cut(body.CandidateID, ":"); found {
		item, err := a.plugins.Resolve(source, sourceID)
		if err != nil || item.AudioURL == "" {
			writeJSON(w, 502, map[string]any{"error": "private_source_failed"})
			return
		}
		a.state.Request(library.Track{
			ID: body.CandidateID, Title: item.Title, Artist: item.Artist,
			Album: item.Album, DurationMS: item.DurationMS, URL: item.AudioURL,
			Source: item.Source, SourceID: item.SourceID,
		}, "admin", "admin")
	} else if track, ok := a.index.Get(body.CandidateID); ok {
		a.state.Request(track, "admin", "admin")
	} else {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) adminTrackDelete(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	var body struct {
		TrackID string `json:"track_id"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	track, existed := a.index.Get(body.TrackID)
	if a.state.Current().ID == body.TrackID {
		a.state.Skip()
	}
	a.index.Remove(body.TrackID)
	if existed && track.Path != "" {
		path := filepath.Clean(track.Path)
		root := filepath.Clean(a.uploadsDir) + string(os.PathSeparator)
		if strings.HasPrefix(path, root) {
			_ = os.Remove(path)
		}
	}
	a.state.Rebuild()
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) adminHideMessage(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	var body struct {
		ID string `json:"id"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	a.state.DeleteMessage(body.ID)
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *api) adminSettings(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, 200, map[string]any{"ok": true, "settings": a.state.Settings()})
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	a.state.UpdateSettings(body)
	writeJSON(w, 200, map[string]any{"ok": true, "settings": a.state.Settings()})
}

func (a *api) adminPolicy(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, 200, map[string]any{"ok": true, "policy": a.state.Policy()})
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	a.state.UpdatePolicy(body)
	writeJSON(w, 200, map[string]any{"ok": true, "policy": a.state.Policy()})
}

func (a *api) adminDeleteMessage(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	var body struct{ ID string `json:"id"` }
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, 400, map[string]any{"error": "bad_json"})
		return
	}
	if !a.state.DeleteMessage(body.ID) {
		writeJSON(w, 404, map[string]any{"error": "not_found"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func isBilibiliLookup(query string) bool {
	value := strings.ToLower(strings.TrimSpace(query))
	if strings.Contains(value, "bilibili.com") || strings.Contains(value, "b23.tv") {
		return true
	}
	if strings.HasPrefix(value, "bv") && len(value) >= 12 {
		return true
	}
	if strings.HasPrefix(value, "av") && len(value) > 2 {
		for _, char := range value[2:] {
			if char < '0' || char > '9' {
				return false
			}
		}
		return true
	}
	return false
}

func (a *api) searchAll(w http.ResponseWriter, r *http.Request) {
	if !a.allow("search", a.clientIP(r), 20, time.Minute) {
		a.rateLimited(w, "搜索")
		return
	}
	query := r.URL.Query().Get("q")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 30 {
		limit = 12
	}
	tracks := a.index.Search(query, limit)
	items := make([]map[string]any, 0, len(tracks)+limit)
	for _, track := range tracks {
		items = append(items, map[string]any{
			"id": track.ID, "source": "local", "source_id": track.ID,
			"title": track.Title, "artist": track.Artist, "album": track.Album,
			"duration_ms": track.DurationMS, "cover_url": track.CoverURL, "candidate_id": track.ID,
		})
	}
	if a.plugins != nil && strings.TrimSpace(query) != "" {
		for _, item := range a.plugins.Search(query) {
			if strings.EqualFold(item.Source, "bilibili") && !isBilibiliLookup(query) {
				// Bilibili is link/BV-only; keyword search must not expose
				// arbitrary Bilibili results.
				continue
			}
			a.pluginMu.Lock()
			a.pluginCache[item.ID] = item
			a.pluginMu.Unlock()
			items = append(items, map[string]any{
				"id": item.ID, "source": item.Source, "source_id": item.SourceID,
				"title": item.Title, "artist": item.Artist, "album": item.Album,
				"duration_ms": item.DurationMS, "cover_url": item.CoverURL, "candidate_id": item.ID,
			})
		}
	}
	// The private helper returns NetEase, QQ and Bilibili in source order.
	// Truncating that flat list hid Bilibili whenever the default result limit
	// was smaller than the NetEase+QQ prefix. Round-robin the sources so every
	// enabled source gets representation in the visible result window.
	groups := map[string][]map[string]any{}
	for _, item := range items {
		source := fmt.Sprint(item["source"])
		groups[source] = append(groups[source], item)
	}
	order := []string{"local", "bilibili", "netease", "qq"}
	seenOrder := map[string]bool{}
	for _, source := range order { seenOrder[source] = true }
	for source := range groups {
		if !seenOrder[source] { order = append(order, source) }
	}
	mixed := make([]map[string]any, 0, limit)
	for round := 0; len(mixed) < limit; round++ {
		added := false
		for _, source := range order {
			group := groups[source]
			if round >= len(group) { continue }
			mixed = append(mixed, group[round])
			added = true
			if len(mixed) >= limit { break }
		}
		if !added { break }
	}
	writeJSON(w, 200, map[string]any{"items": mixed, "candidates": mixed, "results": mixed})
}

func main() {
	dirs := strings.Split(env("RADIO_LIBRARY_DIRS", ""), string(os.PathListSeparator))
	clean := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if strings.TrimSpace(dir) != "" {
			clean = append(clean, strings.TrimSpace(dir))
		}
	}
	libraryFile := env("RADIO_GO_LIBRARY_CONFIG", "data/station-go-library.json")
	if raw, err := os.ReadFile(libraryFile); err == nil {
		var saved []string
		if json.Unmarshal(raw, &saved) == nil && len(saved) > 0 {
			clean = saved
		}
	}
	backgroundsDir := env("RADIO_GO_BACKGROUNDS", "data/backgrounds")
	if err := os.MkdirAll(backgroundsDir, 0o755); err != nil {
		log.Printf("backgrounds dir: %v", err)
	}
	uploadsDir := env("RADIO_GO_UPLOADS", "data/uploads")
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		log.Printf("uploads dir: %v", err)
	}
	clean = append(clean, uploadsDir)
	index := library.New()
	if len(clean) > 0 {
		if count, err := index.Scan(clean, 50000); err != nil {
			log.Printf("initial library scan: %v", err)
		} else {
			log.Printf("library indexed %d tracks", count)
		}
	}
	state := station.New(index, env("RADIO_STATION_NAME", "Go Radio Station"))
	state.Rebuild()
	state.SetPersistPath(env("RADIO_GO_STATE", "data/station-go-state.json"))
	if os.Getenv("RADIO_GO_KEEP_UPLOADS") != "1" {
		state.SetAdvanceHook(func(track library.Track) {
			path := filepath.Clean(track.Path)
			root := filepath.Clean(uploadsDir) + string(os.PathSeparator)
			if path == "" || !strings.HasPrefix(path, root) {
				return
			}
			_ = os.Remove(path)
			index.Remove(track.ID)
		})
	}
	directoryClient := directory.New(
		env("RADIO_GO_IDENTITY", "data/station-go-identity.json"),
		env("RADIO_DIRECTORY_PUBLIC_URL", ""),
		env("RADIO_DIRECTORY_URL", ""),
		os.Getenv("RADIO_DIRECTORY_REGISTER_TOKEN"),
		0, func() string { return state.Name() },
	)
	heartbeat := 5 * time.Minute
	if raw := os.Getenv("RADIO_DIRECTORY_HEARTBEAT"); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
			heartbeat = time.Duration(seconds) * time.Second
		}
	}
	directoryClient.Heartbeat = heartbeat
	directoryClient.Start()
	mediaMode := env("RADIO_MEDIA_MODE", "direct")
	meshHub := mesh.NewHub()
	meshHub.SetLocalSeedOnly(mediaMode == "p2p")
	a := &api{
		index:      index,
		libraryDir: clean,
		adminToken: os.Getenv("RADIO_GO_ADMIN_TOKEN"),
		state:      state,
		source:     stream.NewHub(),
		mesh:       meshHub,
		meshSTUN:   os.Getenv("RADIO_MESH_STUN"),
		mediaMode:  mediaMode,
		staticDir:  env("RADIO_STATIC_DIR", "app/static"),
		uploadsDir: env("RADIO_GO_UPLOADS", "data/uploads"),
		directory: directoryClient,
		joinRequired: os.Getenv("RADIO_JOIN_REQUIRED") == "1",
		joinSecret: env("RADIO_JOIN_SECRET", env("RADIO_GO_ADMIN_TOKEN", "go-join-secret")),
		plugins:    plugins.New(env("RADIO_PRIVATE_PYTHON", "python3"), env("RADIO_PRIVATE_HELPER", "private/plugins/commercial/helper.py")),
		backgroundsDir: env("RADIO_GO_BACKGROUNDS", "data/backgrounds"),
		backgroundFile: env("RADIO_GO_BACKGROUND_URLS", "data/station-go-backgrounds.json"),
		libraryFile:   libraryFile,
		pluginCache: map[string]plugins.Candidate{},
		listeners:   map[string]time.Time{},
		listenerNames: map[string]string{},
		mp3Cache:      map[string]bool{},
		ownerIPs:      ownerSet(env("RADIO_OWNER_IPS", "127.0.0.1,::1")),
		limits:        map[string]map[string][]int64{},
		bannedWords:   splitWords(os.Getenv("RADIO_BANNED_WORDS")),
	}
	if raw, err := os.ReadFile(a.backgroundFile); err == nil {
		_ = json.Unmarshal(raw, &a.backgroundURLs)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.health)
	mux.HandleFunc("/now.json", a.now)
	mux.HandleFunc("/api/listeners", a.listenersAPI)
	mux.HandleFunc("/api/state", a.stateJSON)
	mux.HandleFunc("/api/config", a.config)
	mux.HandleFunc("/api/join", a.joinAPI)
	mux.HandleFunc("/api/request", a.request)
	mux.HandleFunc("/api/queue/cancel", a.queueCancel)
	mux.HandleFunc("/api/queue/pin", a.queuePin)
	mux.HandleFunc("/api/upload", a.upload)
	mux.HandleFunc("/media/uploads/", a.mediaUpload)
	mux.HandleFunc("/api/messages", a.messages)
	mux.HandleFunc("/api/vote-skip", a.voteSkip)
	mux.HandleFunc("/api/report", a.report)
	mux.HandleFunc("/api/lyrics/", a.lyrics)
	mux.HandleFunc("/api/admin/owner", a.adminOwner)
	mux.HandleFunc("/api/admin/state", a.adminState)
	mux.HandleFunc("/api/admin/directory", a.adminDirectory)
	mux.HandleFunc("/api/admin/directory/invite", a.adminDirectory)
	mux.HandleFunc("/api/admin/background", a.adminBackground)
	mux.HandleFunc("/api/admin/background/delete", a.adminBackgroundDelete)
	mux.HandleFunc("/api/admin/background/url", a.adminBackgroundURL)
	mux.HandleFunc("/api/admin/background/url/delete", a.adminBackgroundURL)
	mux.HandleFunc("/backgrounds/", a.backgrounds)
	mux.HandleFunc("/api/admin/settings", a.adminSettings)
	mux.HandleFunc("/api/admin/clear-queue", a.adminClearQueue)
	mux.HandleFunc("/api/admin/queue/delete", a.adminQueueDelete)
	mux.HandleFunc("/api/admin/track", a.adminTrack)
	mux.HandleFunc("/api/admin/track/delete", a.adminTrackDelete)
	mux.HandleFunc("/api/admin/hide-message", a.adminHideMessage)
	mux.HandleFunc("/api/admin/policy", a.adminPolicy)
	mux.HandleFunc("/api/admin/messages/delete", a.adminDeleteMessage)
	mux.HandleFunc("/api/admin/skip", a.skip)
	mux.HandleFunc("/api/admin/pause", a.pause)
	mux.HandleFunc("/api/admin/resume", a.resume)
	mux.HandleFunc("/api/admin/directory/rotate", a.adminDirectory)
	mux.HandleFunc("/api/admin/directory/refresh", a.adminDirectory)
	mux.HandleFunc("/api/cover", a.coverProxy)
	mux.HandleFunc("/api/local-cover/", a.localCover)
	mux.HandleFunc("/native/", a.native)
	mux.HandleFunc("/ws", a.mesh.ServeHTTP)
	mux.HandleFunc("/mesh.json", a.meshStatus)
	mux.HandleFunc("/source/manifest", a.sourceManifest)
	mux.HandleFunc("/source/chunk", a.sourceChunk)
	mux.HandleFunc("/api/library/search", a.search)
	mux.HandleFunc("/api/search", a.searchAll)
	mux.HandleFunc("/", a.root)
	mux.HandleFunc("/api/library", a.list)
	mux.HandleFunc("/api/library/track/", a.track)
	mux.HandleFunc("/api/library/stream/", a.stream)
	mux.HandleFunc("/api/admin/library", a.adminLibrary)
	mux.HandleFunc("/api/admin/library/scan", a.adminLibraryScan)
	addr := env("RADIO_GO_ADDR", "127.0.0.1:8091")
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			entries, err := os.ReadDir(uploadsDir)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasPrefix(entry.Name(), "upload-") {
					continue
				}
				info, err := entry.Info()
				if err == nil && time.Since(info.ModTime()) > 24*time.Hour {
					_ = os.Remove(filepath.Join(uploadsDir, entry.Name()))
				}
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			state.Tick()
		}
	}()
	server := &http.Server{
		Addr:              addr,
		Handler:           a.secure(mux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	log.Printf("radio station go listening on %s", addr)
	log.Fatal(server.ListenAndServe())
}
