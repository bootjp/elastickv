package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/cockroachdb/errors"
)

// dekSize is the DEK length every kek.Wrapper accepts: 32 bytes for
// AES-256-GCM, matching the storage and raft envelope ciphers.
const dekSize = 32

// wrapDEKTimeout bounds the KEK provider round-trip for the remote
// providers (KMS / Vault); the file and env providers return
// immediately.
const wrapDEKTimeout = 30 * time.Second

// runEncryptionWrapDEK implements `elastickv-admin encryption wrap-dek`.
// It draws a fresh random 32-byte DEK, wraps it under the configured
// KEK source (one of --kek-file, --kek-uri, or ELASTICKV_KEK_BASE64 —
// the same resolution rule the server applies to --kekFile /
// --kekUri), and prints the wrapped blob as standard base64 so it can
// be pasted straight into `encryption bootstrap --wrapped-storage-dek`
// / `--wrapped-raft-dek` or `rotate-dek --wrapped-new-dek`.
//
// The cleartext DEK is never written anywhere: it lives only in this
// process's memory between rand.Read and Wrap. The server unwraps it
// under the same KEK at FSM apply time (§5.6 / §5.2).
//
// No RPC — this is a local operation so operators can produce the
// bootstrap material on a workstation that holds the KEK without
// exposing the KEK to the cluster hosts' shells.
func runEncryptionWrapDEK(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("encryption wrap-dek", flag.ContinueOnError)
	kekFile := fs.String("kek-file", "", "§5.1 KEK file path (32 raw bytes, owner-only mode)")
	kekURI := fs.String("kek-uri", "", "§5.1 remote KEK URI: aws-kms://<key-arn>, gcp-kms://<crypto-key-resource>, or vault-transit://<mount>/<key>")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errors.Wrap(err, "parse wrap-dek flags")
	}
	ctx, cancel := context.WithTimeout(context.Background(), wrapDEKTimeout)
	defer cancel()
	wrapper, err := kek.NewWrapperFromSources(ctx, *kekFile, *kekURI)
	if err != nil {
		return errors.Wrap(err, "load KEK source")
	}
	if wrapper == nil {
		return errors.New("no KEK source configured: set --kek-file, --kek-uri, or " + kek.EnvVar)
	}
	wrapped, err := wrapFreshDEK(wrapper)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, base64.StdEncoding.EncodeToString(wrapped)); err != nil {
		return errors.Wrap(err, "write wrapped dek")
	}
	return nil
}

// wrapFreshDEK draws a random DEK and wraps it under w. Split out of
// runEncryptionWrapDEK so tests can exercise the wrap path with an
// in-memory Wrapper without touching the flag parser.
func wrapFreshDEK(w kek.Wrapper) ([]byte, error) {
	dek := make([]byte, dekSize)
	if _, err := rand.Read(dek); err != nil {
		return nil, errors.Wrap(err, "draw random dek")
	}
	wrapped, err := w.Wrap(dek)
	if err != nil {
		return nil, errors.Wrapf(err, "wrap dek under %s", w.Name())
	}
	return wrapped, nil
}
