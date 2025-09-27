package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/cherts/pgscv/internal/log"
)

func hash(args ...any) string {
	var s string
	for _, v := range args {
		s += fmt.Sprintf("%v", v)
	}

	hash := sha256.New()
	_, err := hash.Write([]byte(s))
	if err != nil {
		log.Error(fmt.Sprintf("Error calculating hash: %v", err))
		return ""
	}
	hashKey := hex.EncodeToString(hash.Sum(nil))
	return hashKey
}
