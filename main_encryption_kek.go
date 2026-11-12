package main

import (
	"context"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
)

func loadKEKAfterPreNonceStartupGuards(
	cfg runtimeConfig, unwrapObserver monitoring.KEKUnwrapObserver,
) (kek.Wrapper, error) {
	if err := checkEnvelopeCutoverDivergenceBeforeNonceBump(
		*raftId,
		*raftDir,
		cfg.groups,
		cfg.defaultGroup,
		cfg.multi,
		*encryptionSidecarPath,
		*encryptionEnabled,
	); err != nil {
		return nil, err
	}
	return loadKEKAndRunStartupGuards(unwrapObserver)
}

// loadKEKAndRunStartupGuards loads the configured KEK wrapper and
// runs the §9.1 startup-refusal guards (Stage 6C-1) BEFORE
// buildShardGroups constructs any Raft engine or storage state. The
// two operations are paired in a single helper because the guards
// need the loaded KEK to verify each wrapped DEK in the sidecar
// unwraps cleanly under the configured KEK (ErrKEKMismatch), and
// pairing keeps run()'s top-level branch count under the cyclop
// budget.
//
// The triple gate in Stage 6B-2 (encryptionMutatorsEnabled) is
// the RPC-boundary guard; this helper is the process-boundary
// guard. Both layers exist by design — the RPC gate keeps
// unreachable mutator paths unreachable, the startup gate keeps
// misconfigured nodes from booting at all.
//
// Scope of this pre-engine guard layer is documented in
// internal/encryption/startup.go's CheckStartupGuards godoc and in
// docs/design/2026_04_29_partial_data_at_rest_encryption.md. The
// Stage 6C-3 membership/registry guards run next inside
// buildShardGroupsWithEncryptionWiring, still before Raft engine
// startup; the sidecar-behind-raft-log gap guard remains later
// because it needs an opened engine's applied index and scanner.
func loadKEKAndRunStartupGuards(unwrapObserver monitoring.KEKUnwrapObserver) (kek.Wrapper, error) {
	kekWrapper, err := loadKEKWrapperFromFlag()
	if err != nil {
		return nil, err
	}
	// Decorate BEFORE the guards run. CheckStartupGuards and
	// kek.VerifyWrapper both perform real unwrap round trips — on a
	// fresh node the preflight unwrap can be the only one that ever
	// happens — so decorating after them would leave
	// elastickv_encryption_kek_unwrap_seconds empty despite completed
	// KMS calls.
	kekWrapper = monitoring.NewTimedKEKUnwrapper(kekWrapper, unwrapObserver)
	// Memoize unwraps across the startup phase. The §9.1 guards and
	// HydrateKeystoreFromSidecar each unwrap every wrapped DEK, which
	// was free under the file KEK but is a doubled network round-trip
	// per DEK now that Stage 9B shipped the KMS providers.
	//
	// The cache sits OUTSIDE the timer on purpose: a cache hit must
	// not be recorded as a zero-duration KMS call, which would flatten
	// elastickv_encryption_kek_unwrap_seconds.
	kekWrapper = encryption.NewStartupUnwrapCache(kekWrapper)
	if err := encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: *encryptionEnabled,
		KEKConfigured:     kekWrapper != nil,
		KEK:               kekWrapper,
		SidecarPath:       *encryptionSidecarPath,
	}); err != nil {
		return nil, errors.Wrap(err, "encryption startup guards refused process start")
	}
	if err := verifyKEKBeforeMutators(kekWrapper); err != nil {
		return nil, err
	}
	return kekWrapper, nil
}

func verifyKEKBeforeMutators(wrapper kek.Wrapper) error {
	if !*encryptionEnabled || *encryptionSidecarPath == "" || wrapper == nil {
		return nil
	}
	if err := kek.VerifyWrapper(wrapper); err != nil {
		return errors.Wrap(err, "encryption KEK preflight refused process start")
	}
	return nil
}

// loadKEKWrapperFromFlag constructs the configured KEK wrapper from the
// mutually-exclusive file, URI, or environment source.
// Returns the kek.Wrapper interface rather than the concrete
// *kek.FileWrapper so the call site (buildShardGroups → applier)
// stays decoupled from the file-mode provider — Stage 9 KMS
// providers (AWS KMS, GCP KMS, Vault) will satisfy the same
// interface and slot in without rewriting the dispatch site.
func loadKEKWrapperFromFlag() (kek.Wrapper, error) {
	w, err := kek.NewWrapperFromSources(context.Background(), *kekFile, *kekURI)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load KEK source")
	}
	return w, nil
}

// applierOptionsFor assembles the variadic ApplierOption slice
// for encryption.NewApplier based on which Stage 6B-2 dependencies
// the operator has wired at startup. Each nil/empty input
// suppresses its option, leaving the applier in the Stage 6A
// posture for that axis. Extracted from buildShardGroups so the
// per-shard loop stays under the cyclop complexity budget.
//
// kekWrapper is typed as encryption.KEKUnwrapper (the
// applier-side narrow interface) rather than the wider
// kek.Wrapper so the helper stays decoupled from the wrap-side
// path. Any kek.Wrapper satisfies encryption.KEKUnwrapper
// structurally because both declare Unwrap with the same
// signature.
func applierOptionsFor(kekWrapper encryption.KEKUnwrapper, keystore *encryption.Keystore, sidecarPath string) []encryption.ApplierOption {
	const maxOpts = 3
	opts := make([]encryption.ApplierOption, 0, maxOpts)
	if kekWrapper != nil {
		opts = append(opts, encryption.WithKEK(kekWrapper))
	}
	if keystore != nil {
		opts = append(opts, encryption.WithKeystore(keystore))
	}
	if sidecarPath != "" {
		opts = append(opts, encryption.WithSidecarPath(sidecarPath))
	}
	return opts
}
