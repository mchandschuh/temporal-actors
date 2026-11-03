package actor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	defaultTypeNameMaxBytes = 64
	typeNameHashSuffixBytes = 4
)

// DefaultTypeNameEncoder is the shared stateless encoder used by actor
// definitions. It preserves short safe names. Longer names retain a
// deterministic prefix and replace their final four characters with the final
// four hexadecimal characters of a SHA-256 digest.
func DefaultTypeNameEncoder(typeName string) (string, error) {
	if err := validateTypeName(typeName); err != nil {
		return "", err
	}
	if len(typeName) <= defaultTypeNameMaxBytes {
		return typeName, nil
	}

	digest := sha256.Sum256([]byte(typeName))
	suffix := hex.EncodeToString(digest[:])
	return typeName[:defaultTypeNameMaxBytes-typeNameHashSuffixBytes] + suffix[len(suffix)-typeNameHashSuffixBytes:], nil
}

func validateTypeName(typeName string) error {
	if typeName == "" {
		return errors.New("actor definition type name must not be empty")
	}
	if !utf8.ValidString(typeName) {
		return errors.New("actor definition type name must be valid UTF-8")
	}
	if strings.TrimSpace(typeName) != typeName {
		return fmt.Errorf("actor definition type name must not have leading or trailing whitespace: %q", typeName)
	}
	for _, char := range typeName {
		if char < 0x21 || char > 0x7e || char == '/' {
			return fmt.Errorf("actor definition type name contains unsafe character %q", char)
		}
	}
	return nil
}
