package main

import (
	"context"
	"flag"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
)

// encryptionFlags configures data-at-rest encryption.
//
// sidecarPath gates ONLY the read-only EncryptionAdmin capability surface,
// which is reachable on every shard's gRPC listener so the §7.1 Phase-0
// GetCapability fan-out can poll any member: empty reports
// encryption_capable=false (the §7.1 cutover refuses with
// ErrCapabilityCheckFailed); set, capability probing reads the §5.1
// keys.json and reports encryption_capable=true.
//
// The mutating RPCs (BootstrapEncryption / RotateDEK /
// RegisterEncryptionWriter) additionally require enabled and a successfully
// loaded KEK source (encryptionMutatorsEnabled). Setting sidecarPath alone
// does not enable them. With either condition false,
// registerEncryptionAdminServer omits the Proposer + LeaderView options and
// every mutator short-circuits at the gRPC boundary with FailedPrecondition
// before any Raft proposal is created.
//
// rotateOnStartup is a request, not a guarantee: only the leader of the
// default encryption Raft group proposes the rotation; followers keep the
// request in memory and fire it only if they acquire leadership during this
// process uptime.
//
// kekFile and kekURI are the KEK sources. The KEK never appears in
// elastickv's data dir; it is held externally and exercised only at process
// boot and at DEK bootstrap/rotation per §5.1. With no source the applier's
// ApplyBootstrap and ApplyRotation paths return ErrKEKNotConfigured at apply
// time, which the mutator gate masks at the RPC boundary.
type encryptionFlags struct {
	sidecarPath     string
	enabled         bool
	rotateOnStartup bool
	kekFile         string
	kekURI          string
}

func (f *encryptionFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.sidecarPath, "encryptionSidecarPath", "", "§5.1 keys.json path; enables read-only EncryptionAdmin capability probing. Mutating RPCs additionally require --encryption-enabled and one loaded KEK source.")
	fs.BoolVar(&f.enabled, "encryption-enabled", false, "§6.5 opt-in to encryption-mutating EncryptionAdmin RPCs. Requires exactly one KEK source from --kekFile, --kekUri, or ELASTICKV_KEK_BASE64. Default off.")
	fs.BoolVar(&f.rotateOnStartup, "encryption-rotate-on-startup", false, "§6.5 request a one-shot DEK rotation after this node becomes leader of the default Raft group. Safe for rolling restarts: followers keep the request in memory and only fire if they acquire leadership during this process uptime.")
	fs.StringVar(&f.kekFile, "kekFile", "", "§5.1 KEK file path (32 raw bytes, owner-only mode). When set, the file-backed kek.Wrapper is constructed at startup and threaded into the §6.3 EncryptionApplier so ApplyBootstrap and ApplyRotation can KEK-unwrap.")
	fs.StringVar(&f.kekURI, "kekUri", "", "§5.1 remote KEK URI: aws-kms://<key-arn>, gcp-kms://<crypto-key-resource>, or vault-transit://<mount>/<key>. Mutually exclusive with --kekFile and ELASTICKV_KEK_BASE64.")
}

// configured reports whether the operator opted in to encryption or pointed
// the node at a sidecar.
func (f encryptionFlags) configured() bool {
	return f.enabled || f.sidecarPath != ""
}

// bootstrapRequest is the part of the encryption flags protected bootstrap
// admission depends on.
func (f encryptionFlags) bootstrapRequest() noderuntime.BootstrapEncryptionRequest {
	return noderuntime.BootstrapEncryptionRequest{Configured: f.configured(), RotateOnStartup: f.rotateOnStartup}
}

func loadKEKAfterPreNonceStartupGuards(
	cfg noderuntime.RuntimeConfig, raft raftFlags, f encryptionFlags, unwrapObserver monitoring.KEKUnwrapObserver,
) (kek.Wrapper, error) {
	if err := checkEnvelopeCutoverDivergenceBeforeNonceBump(
		raft.id,
		raft.dataDir,
		cfg.Groups,
		cfg.DefaultGroup,
		cfg.Multi,
		f.sidecarPath,
		f.enabled,
	); err != nil {
		return nil, err
	}
	return loadKEKAndRunStartupGuards(f, unwrapObserver)
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
func loadKEKAndRunStartupGuards(f encryptionFlags, unwrapObserver monitoring.KEKUnwrapObserver) (kek.Wrapper, error) {
	kekWrapper, err := loadKEKWrapperFromFlags(f)
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
		EncryptionEnabled: f.enabled,
		KEKConfigured:     kekWrapper != nil,
		KEK:               kekWrapper,
		SidecarPath:       f.sidecarPath,
	}); err != nil {
		return nil, errors.Wrap(err, "encryption startup guards refused process start")
	}
	if err := verifyKEKBeforeMutators(f, kekWrapper); err != nil {
		return nil, err
	}
	return kekWrapper, nil
}

func verifyKEKBeforeMutators(f encryptionFlags, wrapper kek.Wrapper) error {
	if !f.enabled || f.sidecarPath == "" || wrapper == nil {
		return nil
	}
	if err := kek.VerifyWrapper(wrapper); err != nil {
		return errors.Wrap(err, "encryption KEK preflight refused process start")
	}
	return nil
}

// loadKEKWrapperFromFlags constructs the configured KEK wrapper from the
// mutually-exclusive file, URI, or environment source.
// Returns the kek.Wrapper interface rather than the concrete
// *kek.FileWrapper so the call site (buildShardGroups → applier)
// stays decoupled from the file-mode provider — Stage 9 KMS
// providers (AWS KMS, GCP KMS, Vault) will satisfy the same
// interface and slot in without rewriting the dispatch site.
func loadKEKWrapperFromFlags(f encryptionFlags) (kek.Wrapper, error) {
	w, err := kek.NewWrapperFromSources(context.Background(), f.kekFile, f.kekURI)
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
