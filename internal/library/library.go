package library

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var AudioExtensions = map[string]bool{
	".mp3": true, ".m4a": true, ".aac": true, ".flac": true,
	".ogg": true, ".oga": true, ".opus": true, ".wav": true,
	".webm": true, ".mp4": true,
}

type Track struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Artist      string `json:"artist"`
	Album       string `json:"album"`
	DurationMS  int64  `json:"duration_ms"`
	Format      string `json:"format"`
	Path        string `json:"-"`
	URL         string `json:"-"`
	Source      string `json:"source,omitempty"`
	SourceID    string `json:"source_id,omitempty"`
	CoverURL    string `json:"cover_url,omitempty"`
}

type Index struct {
	mu     sync.RWMutex
	tracks map[string]*Track
	order  []string
}

func New() *Index {
	return &Index{tracks: map[string]*Track{}}
}

func trackID(path string) string {
	sum := sha256.Sum256([]byte(path))
	return hex.EncodeToString(sum[:8])
}

func fileTitle(path string) string {
	base := filepath.Base(path)
	return strings.TrimSpace(strings.TrimSuffix(base, filepath.Ext(base)))
}

// FileMeta is the sidecar record written by /api/upload next to the stored
// audio file. A rescan prefers it over embedded tags so an upload keeps the
// title the listener actually typed, not the generated storage name.
type FileMeta struct {
	Title        string `json:"title,omitempty"`
	Artist       string `json:"artist,omitempty"`
	Album        string `json:"album,omitempty"`
	DurationMS   int64  `json:"duration_ms,omitempty"`
	Nickname     string `json:"nickname,omitempty"`
	OriginalName string `json:"original_name,omitempty"`
	UploadedAt   int64  `json:"uploaded_at,omitempty"`
}

// MetaPath returns the sidecar path for an audio file.
func MetaPath(path string) string { return path + ".json" }

// ReadMeta loads the sidecar for path. Missing or broken sidecars are ignored.
func ReadMeta(path string) (FileMeta, bool) {
	raw, err := os.ReadFile(MetaPath(path))
	if err != nil {
		return FileMeta{}, false
	}
	var meta FileMeta
	if json.Unmarshal(raw, &meta) != nil {
		return FileMeta{}, false
	}
	return meta, true
}

// WriteMeta stores the sidecar for path.
func WriteMeta(path string, meta FileMeta) error {
	raw, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(MetaPath(path), raw, 0o600)
}

// RemoveMeta removes the sidecar for path, ignoring errors.
func RemoveMeta(path string) { _ = os.Remove(MetaPath(path)) }

var (
	toolsOnce  sync.Once
	ffprobeBin string
	ffmpegBin  string
)

func resolveBinaries() {
	ffprobeBin = findBinary("RADIO_FFPROBE", "ffprobe")
	ffmpegBin = findBinary("RADIO_FFMPEG", "ffmpeg")
}

// findBinary resolves an ffmpeg-family tool. Explicit env wins, then PATH, then
// the bundled data/bin and bin directories so a station started from anywhere
// still finds the ffmpeg shipped in a release package.
func findBinary(envKey, name string) string {
	if custom := strings.TrimSpace(os.Getenv(envKey)); custom != "" {
		return custom
	}
	if path, err := exec.LookPath(name); err == nil {
		return path
	}
	candidates := []string{
		filepath.Join("data", "bin", name),
		filepath.Join("bin", name),
	}
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(dir, name),
			filepath.Join(dir, "data", "bin", name),
			filepath.Join(dir, "..", "data", "bin", name),
		)
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() {
			continue
		}
		if abs, err := filepath.Abs(candidate); err == nil {
			return abs
		}
		return candidate
	}
	return ""
}

// ProbeFile reads duration and tags. ffprobe is preferred; when only ffmpeg is
// installed the same facts are parsed out of `ffmpeg -i`, so a minimal package
// without ffprobe still reports correct durations.
func ProbeFile(path string) (title, artist, album string, durationMS int64) {
	toolsOnce.Do(resolveBinaries)
	if ffprobeBin != "" {
		if t, a, al, d, ok := probeFFprobe(path); ok {
			return t, a, al, d
		}
	}
	if ffmpegBin != "" {
		if t, a, al, d, ok := probeFFmpeg(path); ok {
			return t, a, al, d
		}
	}
	return "", "", "", 0
}

func probeFFprobe(path string) (title, artist, album string, durationMS int64, ok bool) {
	cmd := exec.Command(ffprobeBin, "-v", "error",
		"-show_entries", "format=duration:format_tags=title,artist,album",
		"-of", "json", path)
	cmd.WaitDelay = 3 * time.Second
	out, err := cmd.Output()
	if err != nil {
		return "", "", "", 0, false
	}
	var payload struct {
		Format struct {
			Duration string            `json:"duration"`
			Tags     map[string]string `json:"tags"`
		} `json:"format"`
	}
	if json.Unmarshal(out, &payload) != nil {
		return "", "", "", 0, false
	}
	title = pickTag(payload.Format.Tags, "title")
	artist = pickTag(payload.Format.Tags, "artist", "album_artist")
	album = pickTag(payload.Format.Tags, "album")
	if payload.Format.Duration != "" {
		var seconds float64
		if _, err := fmt.Sscanf(payload.Format.Duration, "%f", &seconds); err == nil {
			durationMS = int64(seconds * 1000)
		}
	}
	return title, artist, album, durationMS, true
}

// pickTag matches tag names case-insensitively: FLAC and Ogg rips usually carry
// TITLE/ARTIST/ALBUM in upper case, MP3 ID3 in lower case.
func pickTag(tags map[string]string, names ...string) string {
	for key, value := range tags {
		lower := strings.ToLower(strings.TrimSpace(key))
		for _, name := range names {
			if lower == name {
				if trimmed := strings.TrimSpace(value); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

func probeFFmpeg(path string) (title, artist, album string, durationMS int64, ok bool) {
	// ffmpeg -i exits non-zero without an output target but still prints the
	// input metadata, which is all we need.
	cmd := exec.Command(ffmpegBin, "-hide_banner", "-nostdin", "-i", path)
	cmd.WaitDelay = 3 * time.Second
	out, _ := cmd.CombinedOutput()
	if len(out) == 0 {
		return "", "", "", 0, false
	}
	for _, raw := range strings.Split(string(out), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(line), "duration:") {
			if durationMS == 0 {
				value := strings.TrimSpace(line[len("duration:"):])
				if comma := strings.IndexByte(value, ','); comma >= 0 {
					value = value[:comma]
				}
				durationMS = parseClockMS(strings.TrimSpace(value))
			}
			continue
		}
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		key = strings.TrimPrefix(key, "tag:")
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		switch key {
		case "title":
			if title == "" {
				title = value
			}
		case "artist":
			if artist == "" {
				artist = value
			}
		case "album":
			if album == "" {
				album = value
			}
		}
	}
	if durationMS == 0 && title == "" && artist == "" && album == "" {
		return "", "", "", 0, false
	}
	return title, artist, album, durationMS, true
}

// parseClockMS converts "00:03:07.42" into milliseconds.
func parseClockMS(value string) int64 {
	parts := strings.Split(value, ":")
	if len(parts) == 0 || len(parts) > 3 {
		return 0
	}
	total := 0.0
	for _, part := range parts {
		seconds, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return 0
		}
		total = total*60 + seconds
	}
	if total <= 0 {
		return 0
	}
	return int64(total * 1000)
}

func scanFile(path string) *Track {
	ext := strings.ToLower(filepath.Ext(path))
	if !AudioExtensions[ext] {
		return nil
	}
	title, artist, album, duration := ProbeFile(path)
	meta, hasMeta := ReadMeta(path)
	if hasMeta {
		if meta.Title != "" {
			title = meta.Title
		}
		if meta.Artist != "" {
			artist = meta.Artist
		}
		if meta.Album != "" {
			album = meta.Album
		}
		if meta.DurationMS > 0 {
			duration = meta.DurationMS
		}
	}
	if title == "" {
		title = fileTitle(path)
	}
	id := trackID(path)
	return &Track{
		ID: id, Title: title, Artist: artist, Album: album,
		DurationMS: duration, Format: strings.TrimPrefix(ext, "."),
		Path: path, Source: "local", SourceID: id,
		CoverURL: "/api/local-cover/" + id,
	}
}

// AddFile probes a single audio file and inserts it into the index without
// rescanning (and reshuffling) the whole library.
func (i *Index) AddFile(path string) (Track, bool) {
	track := scanFile(path)
	if track == nil {
		return Track{}, false
	}
	i.Put(track)
	return *track, true
}

// Put inserts or replaces a track in the index.
func (i *Index) Put(track *Track) {
	if track == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.tracks == nil {
		i.tracks = map[string]*Track{}
	}
	if _, exists := i.tracks[track.ID]; !exists {
		i.order = append(i.order, track.ID)
	}
	i.tracks[track.ID] = track
}

func (i *Index) Scan(dirs []string, maxFiles int) (int, error) {
	if maxFiles <= 0 {
		maxFiles = 50000
	}
	found := map[string]*Track{}
	count := 0
	for _, root := range dirs {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		root, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			continue
		}
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return nil
			}
			if entry.Type()&fs.ModeSymlink != 0 {
				return nil
			}
			if entry.IsDir() {
				name := entry.Name()
				if name == ".git" || name == "node_modules" || strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if count >= maxFiles {
				return filepath.SkipAll
			}
			track := scanFile(path)
			if track == nil {
				return nil
			}
			found[track.ID] = track
			count++
			return nil
		})
		if err != nil && !errors.Is(err, filepath.SkipAll) {
			return 0, err
		}
	}
	order := make([]string, 0, len(found))
	for id := range found {
		order = append(order, id)
	}
	sort.Slice(order, func(a, b int) bool {
		left, right := found[order[a]], found[order[b]]
		if strings.ToLower(left.Title) != strings.ToLower(right.Title) {
			return strings.ToLower(left.Title) < strings.ToLower(right.Title)
		}
		return left.ID < right.ID
	})
	i.mu.Lock()
	i.tracks = found
	i.order = order
	i.mu.Unlock()
	return len(found), nil
}

func (i *Index) Search(query string, limit int) []Track {
	query = strings.ToLower(strings.TrimSpace(query))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	results := make([]Track, 0, limit)
	for _, id := range i.order {
		track := i.tracks[id]
		if track == nil {
			continue
		}
		haystack := strings.ToLower(track.Title + " " + track.Artist + " " + track.Album)
		if query != "" && !strings.Contains(haystack, query) {
			continue
		}
		results = append(results, *track)
		if len(results) >= limit {
			break
		}
	}
	return results
}

func (i *Index) List(limit int) []Track {
	// Search is capped for UI/API callers; the station queue rebuild needs
	// the full index. Keep this separate so a large library does not silently
	// become a 20-track rotation after every restart.
	if limit <= 0 || limit > 50000 {
		limit = 50000
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	tracks := make([]Track, 0, min(limit, len(i.order)))
	for _, id := range i.order {
		track := i.tracks[id]
		if track == nil {
			continue
		}
		tracks = append(tracks, *track)
		if len(tracks) >= limit {
			break
		}
	}
	return tracks
}

func (i *Index) Get(id string) (Track, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	track := i.tracks[id]
	if track == nil {
		return Track{}, false
	}
	return *track, true
}

func (i *Index) Count() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.tracks)
}

func (i *Index) Remove(id string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, ok := i.tracks[id]; !ok {
		return false
	}
	delete(i.tracks, id)
	order := i.order[:0]
	for _, item := range i.order {
		if item != id {
			order = append(order, item)
		}
	}
	i.order = order
	return true
}
