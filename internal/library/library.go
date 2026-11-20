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
	"strings"
	"sync"
	"time"
)

var AudioExtensions = map[string]bool{
	".mp3": true, ".m4a": true, ".aac": true, ".flac": true,
	".ogg": true, ".oga": true, ".opus": true, ".wav": true,
	".webm": true,
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

func parseMetadata(path string) (title, artist, album string, durationMS int64) {
	title, artist, album = fileTitle(path), "", ""
	cmd := exec.Command("ffprobe", "-v", "error",
		"-show_entries", "format=duration:format_tags=title,artist,album",
		"-of", "json", path)
	cmd.WaitDelay = 2 * time.Second
	out, err := cmd.Output()
	if err != nil {
		return
	}
	var payload struct {
		Format struct {
			Duration string            `json:"duration"`
			Tags     map[string]string `json:"tags"`
		} `json:"format"`
	}
	if json.Unmarshal(out, &payload) != nil {
		return
	}
	if value := strings.TrimSpace(payload.Format.Tags["title"]); value != "" {
		title = value
	}
	artist = strings.TrimSpace(payload.Format.Tags["artist"])
	album = strings.TrimSpace(payload.Format.Tags["album"])
	if payload.Format.Duration != "" {
		var seconds float64
		if _, err := fmt.Sscanf(payload.Format.Duration, "%f", &seconds); err == nil {
			durationMS = int64(seconds * 1000)
		}
	}
	return
}

func scanFile(path string) *Track {
	ext := strings.ToLower(filepath.Ext(path))
	if !AudioExtensions[ext] {
		return nil
	}
	title, artist, album, duration := parseMetadata(path)
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
