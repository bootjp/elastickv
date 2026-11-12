package main

import (
	"context"
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/cockroachdb/errors"
)

// No runtime globals or environment-only KEK fallback on operator commands.
type bootstrapOfflineEncryptionCLI struct{ Sidecar, KEKFile, KEKURI string }

func (c *bootstrapOfflineEncryptionCLI) flags(fs *flag.FlagSet) {
	for name, target := range map[string]*string{"encryption-sidecar": &c.Sidecar, "encryption-kek-file": &c.KEKFile, "encryption-kek-uri": &c.KEKURI} {
		seen := false
		fs.Func(name, "explicit read-only offline encryption input", func(value string) error {
			if seen || value == "" {
				return errors.New("duplicate or empty offline encryption option")
			}
			seen = true
			*target = value
			return nil
		})
	}
}

func (c bootstrapOfflineEncryptionCLI) validate() error {
	if c == (bootstrapOfflineEncryptionCLI{}) {
		return nil
	}
	if !bootstrapBaselineValidPath(c.Sidecar) || (c.KEKFile == "") == (c.KEKURI == "") {
		return errors.New("offline encryption requires sidecar and exactly one explicit KEK file or URI")
	}
	if c.KEKFile != "" && !bootstrapBaselineValidPath(c.KEKFile) {
		return errors.New("offline KEK file requires canonical absolute path")
	}
	if strings.ContainsAny(c.KEKURI, "\x00\r\n\t ") {
		return errors.New("invalid offline KEK URI")
	}
	if c.Sidecar == c.KEKFile {
		return errors.New("sidecar and KEK must be distinct")
	}
	return nil
}

func (c bootstrapOfflineEncryptionCLI) paths() []string {
	var paths []string
	if c.Sidecar != "" {
		paths = append(paths, c.Sidecar)
	}
	if c.KEKFile != "" {
		paths = append(paths, c.KEKFile)
	}
	return paths
}

func (c bootstrapOfflineEncryptionCLI) load(ctx context.Context, member, group uint64, dataDir string) (*bootstrapOfflineEncryption, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if c == (bootstrapOfflineEncryptionCLI{}) {
		return nil, nil
	}
	if group == noderuntime.DedicatedTSORaftGroupID {
		return nil, errors.Wrap(errBootstrapOfflineStore, "encrypted scalar TSO offline migration is unsupported")
	}
	if c.KEKFile != "" {
		if err := bootstrapOfflineSecretOutsideSource(dataDir, c.KEKFile); err != nil {
			return nil, err
		}
	}
	wrapper, err := kek.NewWrapperFromSources(ctx, c.KEKFile, c.KEKURI)
	if err != nil {
		return nil, errors.Wrap(errBootstrapOfflineStore, "cannot load explicit offline KEK source")
	}
	if err := kek.VerifyWrapper(wrapper); err != nil {
		return nil, errors.Wrap(errBootstrapOfflineStore, "offline KEK verification failed")
	}
	enc, err := newBootstrapOfflineEncryption(bootstrapOfflineEncryptionConfig{SidecarPath: c.Sidecar, Wrapper: wrapper, MemberID: member})
	if err != nil {
		return nil, err
	}
	enc.secretFile = c.KEKFile
	return enc, nil
}

func bootstrapOfflineSecretOutsideSource(dataDir, key string) error {
	source, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	secret, err := filepath.EvalSymlinks(key)
	if err != nil {
		return errors.Wrap(errBootstrapOfflineStore, "offline KEK path unavailable")
	}
	rel, err := filepath.Rel(source, secret)
	if err != nil {
		return errors.WithStack(err)
	}
	if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.Wrap(errBootstrapOfflineStore, "KEK must be outside source and native backup image")
	}
	info, err := bootstrapOfflineLstat(secret)
	if err != nil {
		return errors.Wrap(errBootstrapOfflineStore, "offline KEK unavailable")
	}
	return bootstrapOfflineRejectSecretAliases(source, info)
}

func bootstrapOfflineRejectSecretAliases(source string, secret os.FileInfo) error {
	count := 0
	return bootstrapOfflineWalk(source, func(_ string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errors.WithStack(walkErr)
		}
		count++
		if count > bootstrapOfflineTreeLimit {
			return errors.Wrap(errBootstrapOfflineStore, "source tree bound exceeded during KEK alias check")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return errors.WithStack(err)
		}
		if !info.Mode().IsRegular() || os.SameFile(info, secret) {
			return errors.Wrap(errBootstrapOfflineStore, "source contains secret KEK alias or unsupported file")
		}
		return nil
	})
}

func bootstrapOfflineOptionalEncryption(e *bootstrapOfflineEncryption) []*bootstrapOfflineEncryption {
	if e == nil {
		return nil
	}
	return []*bootstrapOfflineEncryption{e}
}
