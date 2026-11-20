package plugins

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Candidate struct {
	ID         string `json:"id"`
	Source     string `json:"source"`
	SourceID   string `json:"source_id"`
	Title      string `json:"title"`
	Artist     string `json:"artist"`
	Album      string `json:"album"`
	DurationMS int64  `json:"duration_ms"`
	CoverURL   string `json:"cover_url"`
	AudioURL   string `json:"audio_url,omitempty"`
}

type Client struct {
	Python string
	Script string
}

func New(python, script string) *Client {
	if python == "" {
		python = "python3"
	}
	return &Client{Python: python, Script: script}
}

func (c *Client) available() bool {
	if c == nil || c.Script == "" {
		return false
	}
	_, err := os.Stat(c.Script)
	return err == nil
}

func (c *Client) run(args ...string) ([]byte, error) {
	if !c.available() {
		return nil, fmt.Errorf("private plugin unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.Python, append([]string{c.Script}, args...)...)
	return cmd.Output()
}

func (c *Client) Search(query string) []Candidate {
	raw, err := c.run("search", query)
	if err != nil {
		return nil
	}
	var payload struct {
		Items []Candidate `json:"items"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil
	}
	for i := range payload.Items {
		if payload.Items[i].ID == "" && payload.Items[i].Source != "" {
			payload.Items[i].ID = payload.Items[i].Source + ":" + payload.Items[i].SourceID
		}
	}
	return payload.Items
}

func (c *Client) Resolve(source, sourceID string) (Candidate, error) {
	raw, err := c.run("resolve", source, sourceID)
	if err != nil {
		return Candidate{}, err
	}
	var payload struct {
		Item *Candidate `json:"item"`
	}
	if json.Unmarshal(raw, &payload) != nil || payload.Item == nil {
		return Candidate{}, fmt.Errorf("private plugin resolve failed")
	}
	if payload.Item.ID == "" {
		payload.Item.ID = strings.TrimSpace(source) + ":" + strings.TrimSpace(sourceID)
	}
	return *payload.Item, nil
}

func (c *Client) Lyrics(source, sourceID string) (string, string) {
	raw, err := c.run("lyrics", source, sourceID)
	if err != nil {
		return "", ""
	}
	var payload struct {
		LRC         string `json:"lrc"`
		Translation string `json:"translation"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return "", ""
	}
	return payload.LRC, payload.Translation
}
