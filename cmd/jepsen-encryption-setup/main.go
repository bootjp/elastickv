// Command jepsen-encryption-setup produces a KEK-wrapped DEK for the Jepsen
// harness, which needs one to call `elastickv-admin encryption bootstrap`.
//
// This is TEST HARNESS TOOLING, not an operator tool. It exists because
// bootstrap takes the wrapped DEK bytes as an argument -- an operator gets them
// from their KMS -- and the Jepsen harness has only a local KEK file. Keeping it
// here rather than adding an `elastickv-admin` subcommand avoids putting DEK
// generation into operator-facing tooling, where handling raw key material
// needs its own design and review.
//
// The plaintext DEK never leaves this process: it is generated, wrapped under
// the KEK, and only the wrapped form is printed. The wrapped DEK is safe to pass
// on a command line, which is the whole point of the envelope scheme.
package main

import (
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"os"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/cockroachdb/errors"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "jepsen-encryption-setup: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, out *os.File) error {
	fs := flag.NewFlagSet("jepsen-encryption-setup", flag.ContinueOnError)
	kekFile := fs.String("kek-file", "", "path to the §5.1 KEK file (32 raw bytes, owner-only mode)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errors.Wrap(err, "parse flags")
	}
	if *kekFile == "" {
		return errors.New("--kek-file is required")
	}
	wrapped, err := wrapFreshDEK(*kekFile)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, wrapped); err != nil {
		return errors.Wrap(err, "write wrapped dek")
	}
	return nil
}

// wrapFreshDEK generates one AES-256 DEK and returns its base64 wrapped form.
func wrapFreshDEK(kekFile string) (string, error) {
	wrapper, err := kek.NewFileWrapper(kekFile)
	if err != nil {
		return "", errors.Wrap(err, "open kek file")
	}
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		return "", errors.Wrap(err, "generate dek")
	}
	wrapped, err := wrapper.Wrap(dek)
	if err != nil {
		return "", errors.Wrap(err, "wrap dek")
	}
	return base64.StdEncoding.EncodeToString(wrapped), nil
}
