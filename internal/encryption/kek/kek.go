// Package kek implements KEK (Key Encryption Key) providers that wrap
// and unwrap DEKs (Data Encryption Keys) per §5.1 of the data-at-rest
// encryption design.
//
// The KEK never appears in elastickv's data dir. It is held externally —
// in a KMS, a sealed file, or HashiCorp Vault — and only exercised at
// process boot and at DEK rotation.
//
// Stage 0 ships only the FileWrapper for tests / single-host clusters.
// AWS KMS, GCP KMS, and Vault providers are added in Stage 9.
package kek

// Wrapper wraps and unwraps DEK bytes under an externally-held KEK.
//
// Wrap input is always exactly encryption.KeySize (32) bytes; the
// wrapped output's exact size depends on the provider but is at least
// the input size plus an AEAD nonce and tag (or KMS protocol overhead).
//
// Implementations MUST be safe for concurrent use by multiple goroutines
// because the encryption Keystore may issue Wrap/Unwrap from rotation
// and resync paths simultaneously.
type Wrapper interface {
	Wrap(dek []byte) ([]byte, error)
	Unwrap(wrapped []byte) ([]byte, error)

	// Name returns a short identifier of the KEK source ("file",
	// "aws-kms", "gcp-kms", "vault", "env"). Surfaces in logs and the
	// EncryptionAdmin status RPC.
	Name() string
}
