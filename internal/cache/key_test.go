package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected string
	}{
		{
			name:     "string arguments",
			args:     []any{"arg1", "arg2"},
			expected: hex.EncodeToString(sha256.New().Sum([]byte("arg1arg2"))),
		},
		{
			name:     "mixed arguments",
			args:     []any{"string", 123, true},
			expected: hex.EncodeToString(sha256.New().Sum([]byte("string123true"))),
		},
		{
			name:     "empty arguments",
			args:     []any{},
			expected: hex.EncodeToString(sha256.New().Sum(nil)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hash(tt.args...)
			assert.NotEmpty(t, result)
			assert.Len(t, result, 64) // SHA256 hash should be 64 chars
		})
	}
}

func TestHashConsistency(t *testing.T) {
	hash1 := hash("test", 123, true)
	hash2 := hash("test", 123, true)
	assert.Equal(t, hash1, hash2)

	hash3 := hash("test", 124, true)
	assert.NotEqual(t, hash1, hash3)
}
