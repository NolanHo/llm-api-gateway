package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

type CarrierHasher struct {
	key []byte
}

func NewCarrierHasher(secret string) CarrierHasher {
	return CarrierHasher{key: []byte(secret)}
}

func (h CarrierHasher) Sum(value string) string {
	if value == "" {
		return ""
	}
	mac := hmac.New(sha256.New, h.key)
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}
