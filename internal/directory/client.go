package directory

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const protocol = "radio-station/1"

type Identity struct {
	StationID  string `json:"station_id"`
	PublicKey  string `json:"public_key"`
	InviteCode string `json:"invite_code"`
}

type Status struct {
	Enabled   bool   `json:"enabled"`
	Registered bool  `json:"registered"`
	StationID string `json:"station_id"`
	PublicURL string `json:"public_url"`
	InviteCode string `json:"invite_code,omitempty"`
	LastError string `json:"last_error"`
	LastSuccess int64 `json:"last_success"`
}

type Client struct {
	URL        string
	Token      string
	PublicURL  string
	Heartbeat  time.Duration
	Identity   Identity
	dataFile   string
	name       func() string
	client     *http.Client
	mu         sync.Mutex
	registered bool
	lastError  string
	lastOK     int64
	stop       chan struct{}
}

var alphabet = []byte("0123456789ABCDEFGHJKMNPQRSTVWXYZ")

func normalize(raw string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(raw) {
		switch r {
		case 'I', 'L':
			r = '1'
		case 'O':
			r = '0'
		case 'U':
			r = 'V'
		}
		if strings.ContainsRune(string(alphabet), r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func CodeHash(code string) string {
	sum := sha256.Sum256([]byte("radio-invite/1:" + normalize(code)))
	return "sha256:" + base64.RawURLEncoding.EncodeToString(sum[:])
}

func newCode() string {
	code := make([]byte, 16)
	seed := time.Now().UnixNano()
	for i := range code {
		seed = seed*6364136223846793005 + 1442695040888963407
		code[i] = alphabet[uint64(seed)>>33%uint64(len(alphabet))]
	}
	return string(code)
}

func newPublicKey() string {
	seed := sha256.Sum256([]byte(fmt.Sprintf("go-station-key-%d", time.Now().UnixNano())))
	return "ed25519:" + base64.RawURLEncoding.EncodeToString(seed[:])
}

func New(dataFile, publicURL, baseURL, token string, heartbeat time.Duration, name func() string) *Client {
	c := &Client{
		URL: strings.TrimRight(baseURL, "/"), Token: token,
		PublicURL: strings.TrimRight(publicURL, "/"), Heartbeat: heartbeat,
		dataFile: dataFile, name: name, client: &http.Client{Timeout: 12 * time.Second},
	}
	if heartbeat <= 0 {
		c.Heartbeat = 5 * time.Minute
	}
	if raw, err := os.ReadFile(dataFile); err == nil {
		_ = json.Unmarshal(raw, &c.Identity)
	}
	if c.Identity.StationID == "" {
		c.Identity.StationID = "st_" + strings.TrimPrefix(fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))), "")[:12]
	}
	if c.Identity.PublicKey == "" {
		c.Identity.PublicKey = newPublicKey()
	}
	if c.Identity.InviteCode == "" {
		c.Identity.InviteCode = newCode()
	}
	c.save()
	return c
}

func (c *Client) save() {
	_ = os.MkdirAll(filepath.Dir(c.dataFile), 0o755)
	raw, _ := json.MarshalIndent(c.Identity, "", "  ")
	_ = os.WriteFile(c.dataFile, raw, 0o600)
}

func (c *Client) Enabled() bool {
	return c.URL != "" && c.Token != "" && c.PublicURL != ""
}

func (c *Client) InviteCode() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Identity.InviteCode
}

func (c *Client) Verify(raw string) bool {
	supplied := normalize(raw)
	expected := normalize(c.InviteCode())
	if supplied == "" || expected == "" {
		return false
	}
	if len(supplied) == 16 {
		return subtle.ConstantTimeCompare([]byte(supplied), []byte(expected)) == 1
	}
	return strings.Contains(supplied, expected)
}

func (c *Client) descriptor() map[string]any {
	return map[string]any{
		"protocol": protocol, "station_id": c.Identity.StationID,
		"name": c.name(), "public_key": c.Identity.PublicKey,
		"description": "", "capabilities": []string{"join", "mesh", "local-library"},
		"connectivity": []string{"direct"},
		"endpoints": []map[string]any{{"kind": "direct", "url": c.PublicURL}},
		"issued_at": time.Now().UTC().Format(time.RFC3339),
	}
}

func (c *Client) request(path string, payload any) error {
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, c.URL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("User-Agent", "radio-go-station/1")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("directory HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Client) Refresh() error {
	c.mu.Lock()
	registered := c.registered
	c.mu.Unlock()
	var err error
	if !registered {
		err = c.request("/v1/stations", map[string]any{
			"station": c.descriptor(), "invite_hashes": []string{CodeHash(c.InviteCode())},
		})
	} else {
		err = c.request("/v1/stations/"+url.PathEscape(c.Identity.StationID)+"/heartbeat", map[string]any{
			"endpoints": []map[string]any{{"kind": "direct", "url": c.PublicURL}},
			"connectivity": []string{"direct"},
		})
		if err != nil {
			// Retry registration once; the Directory may have been reset.
			if registerErr := c.request("/v1/stations", map[string]any{
				"station": c.descriptor(), "invite_hashes": []string{CodeHash(c.InviteCode())},
			}); registerErr == nil {
				err = nil
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.lastError = err.Error()
		c.registered = false
		return err
	}
	c.registered = true
	c.lastError = ""
	c.lastOK = time.Now().Unix()
	return nil
}

func (c *Client) Start() {
	if !c.Enabled() {
		return
	}
	c.stop = make(chan struct{})
	go func() {
		for {
			if err := c.Refresh(); err != nil {
				log.Printf("directory: %v", err)
			}
			select {
			case <-c.stop:
				return
			case <-time.After(c.Heartbeat):
			}
		}
	}()
}

func (c *Client) Stop() {
	if c.stop != nil {
		close(c.stop)
	}
}

func (c *Client) Status(includeCode bool) Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := Status{
		Enabled: c.Enabled(), Registered: c.registered,
		StationID: c.Identity.StationID, PublicURL: c.PublicURL,
		LastError: c.lastError, LastSuccess: c.lastOK,
	}
	if includeCode {
		result.InviteCode = c.Identity.InviteCode
	}
	return result
}

func (c *Client) SetInvite(raw string) error {
	code := normalize(raw)
	if len(code) != 16 {
		return fmt.Errorf("邀请码必须是 16 位")
	}
	for _, char := range code {
		found := false
		for _, allowed := range alphabet {
			if char == rune(allowed) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("邀请码包含非法字符")
		}
	}
	c.mu.Lock()
	oldCode := c.Identity.InviteCode
	oldRegistered := c.registered
	c.Identity.InviteCode = code
	c.registered = false
	c.mu.Unlock()
	c.save()
	if err := c.Refresh(); err != nil {
		c.mu.Lock()
		c.Identity.InviteCode = oldCode
		c.registered = oldRegistered
		c.mu.Unlock()
		c.save()
		return err
	}
	return nil
}

func (c *Client) Rotate() {
	c.mu.Lock()
	oldCode := c.Identity.InviteCode
	oldRegistered := c.registered
	c.Identity.InviteCode = newCode()
	c.registered = false
	c.mu.Unlock()
	c.save()
	if err := c.Refresh(); err != nil {
		c.mu.Lock()
		c.Identity.InviteCode = oldCode
		c.registered = oldRegistered
		c.mu.Unlock()
		c.save()
	}
}
