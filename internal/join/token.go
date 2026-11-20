package join

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/Xieal1122/radio-go/internal/directory"
)

const CookieName = "radio_join"

func key(secret string) []byte {
	sum := sha256.Sum256([]byte("radio-join-secret:" + secret))
	return sum[:]
}

func Make(secret, code string, ttl time.Duration) string {
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	expires := time.Now().Add(ttl).Unix()
	marker := directory.CodeHash(code)[7:23]
	payload := "v1:" + strconv.FormatInt(expires, 10) + ":" + marker
	mac := hmac.New(sha256.New, key(secret))
	_, _ = mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func Verify(secret, token, code string) bool {
	payload, encoded, ok := strings.Cut(token, ".")
	if !ok {
		return false
	}
	parts := strings.Split(payload, ":")
	if len(parts) != 3 || parts[0] != "v1" {
		return false
	}
	expires, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || expires < time.Now().Unix() {
		return false
	}
	if parts[2] != directory.CodeHash(code)[7:23] {
		return false
	}
	mac := hmac.New(sha256.New, key(secret))
	_, _ = mac.Write([]byte(payload))
	supplied, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return false
	}
	return hmac.Equal(mac.Sum(nil), supplied)
}
