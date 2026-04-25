package internal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// LoadBearerTokenFile materialises a bearer-token file with a strict upper
// bound on size so a misconfigured path (for example, pointing at a log)
// cannot force an arbitrary allocation before the bearer-token check.
// The file is read through an io.LimitReader bounded to maxBytes+1 so a
// file that grows or is swapped between stat() and read() still cannot
// sneak past the cap.
//
// The returned string has surrounding whitespace trimmed; an empty file (or
// one that is only whitespace) is reported as an error so operators notice
// the misconfiguration immediately.
//
// The humanName is used in error messages to distinguish token files (e.g.
// "admin token" vs "node token"); callers typically pass a fixed string like
// "admin token" or "node token".
func LoadBearerTokenFile(path string, maxBytes int64, humanName string) (string, error) {
	if humanName == "" {
		humanName = "token"
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", errors.Wrapf(err, "resolve %s path", humanName)
	}
	f, err := os.Open(abs)
	if err != nil {
		return "", errors.Wrapf(err, "open %s file", humanName)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			log.Printf("internal: close %s file %s: %v", humanName, abs, cerr)
		}
	}()
	b, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return "", errors.Wrapf(err, "read %s file", humanName)
	}
	if int64(len(b)) > maxBytes {
		return "", fmt.Errorf("%s file %s exceeds maximum of %d bytes", humanName, abs, maxBytes)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("%s file %s is empty", humanName, abs)
	}
	return tok, nil
}
