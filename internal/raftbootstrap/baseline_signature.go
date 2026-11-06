package raftbootstrap

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
)

const (
	baselineBlobLimit       = 16 << 10
	baselineKeyLimit        = baselineBlobLimit
	baselineSignatureLimit  = baselineBlobLimit
	baselineSignatureDomain = "elastickv/bootstrap/baseline-evidence/v1\x00"
)

// SignBaselineProbe never serializes private key material. Signers use standard
// Ed25519, ECDSA ASN.1, or RSA PKCS#1 v1.5 signatures, with SPKI-pinned keys.
func SignBaselineProbe(p BaselineProbe, signer crypto.Signer) (BaselineProbe, error) {
	if signer == nil {
		return BaselineProbe{}, ErrInvalid
	}
	if err := baselineSize(p.BaselineContext, []BaselineMemberEvidence{p.Evidence}); err != nil {
		return BaselineProbe{}, err
	}
	c, err := canonicalBaselineContext(p.BaselineContext)
	if err != nil {
		return BaselineProbe{}, err
	}
	p.BaselineContext = c
	der, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return BaselineProbe{}, ErrInvalid
	}
	p.Evidence.PublicKeyDER, p.Evidence.Signature = der, nil
	if err := validateBaselineEvidence(c, p.Evidence, false); err != nil {
		return BaselineProbe{}, err
	}
	if err := baselineSize(c, []BaselineMemberEvidence{p.Evidence}); err != nil {
		return BaselineProbe{}, err
	}
	message := baselineSigningMessage(baselineContextDigest(c), p.Evidence)
	p.Evidence.Signature, err = signBaselineMessage(signer, message)
	if err != nil {
		return BaselineProbe{}, err
	}
	if err := baselineSize(c, []BaselineMemberEvidence{p.Evidence}); err != nil {
		return BaselineProbe{}, err
	}
	if err := verifyBaselineEvidence(c, p.Evidence); err != nil {
		return BaselineProbe{}, err
	}
	return p, nil
}

func signBaselineMessage(signer crypto.Signer, message []byte) ([]byte, error) {
	input, opts := message, crypto.SignerOpts(crypto.Hash(0))
	switch signer.Public().(type) {
	case ed25519.PublicKey:
	case *ecdsa.PublicKey, *rsa.PublicKey:
		sum := sha256.Sum256(message)
		input, opts = sum[:], crypto.SHA256
	default:
		return nil, ErrUnsupported
	}
	signature, err := signer.Sign(rand.Reader, input, opts)
	if err != nil {
		return nil, ErrInvalid
	}
	return signature, nil
}

func baselineContextDigest(c BaselineContext) Digest {
	w := &encoder{limit: MaxFileSize}
	encodeBaselineContext(w, c)
	return sha256.Sum256(w.Bytes())
}

func baselineSigningMessage(contextDigest Digest, e BaselineMemberEvidence) []byte {
	w := &encoder{limit: MaxFileSize}
	w.appendString(baselineSignatureDomain)
	w.fixed(contextDigest)
	encodeBaselineEvidence(w, e, false)
	return w.Bytes()
}

func verifyBaselineEvidence(c BaselineContext, e BaselineMemberEvidence) error {
	digest, err := ConfigurationDigest(c.Configuration)
	if err != nil {
		return err
	}
	return verifyBaselineEvidenceDigests(c, e, digest, baselineContextDigest(c))
}

func verifyBaselineEvidenceDigests(c BaselineContext, e BaselineMemberEvidence, configurationDigest, contextDigest Digest) error {
	if err := validateBaselineEvidenceDigest(c, e, true, configurationDigest); err != nil {
		return err
	}
	key, err := x509.ParsePKIXPublicKey(e.PublicKeyDER)
	if err != nil {
		return ErrInvalid
	}
	canonical, err := x509.MarshalPKIXPublicKey(key)
	if err != nil || !bytes.Equal(canonical, e.PublicKeyDER) {
		return ErrInvalid
	}
	message := baselineSigningMessage(contextDigest, e)
	return verifyBaselineSignature(key, message, e.Signature)
}

func verifyBaselineSignature(key crypto.PublicKey, message, signature []byte) error {
	sum := sha256.Sum256(message)
	switch k := key.(type) {
	case ed25519.PublicKey:
		if ed25519.Verify(k, message, signature) {
			return nil
		}
	case *ecdsa.PublicKey:
		if ecdsa.VerifyASN1(k, sum[:], signature) {
			return nil
		}
	case *rsa.PublicKey:
		if k.N.BitLen() >= 2048 && k.N.BitLen() <= 8192 && rsa.VerifyPKCS1v15(k, crypto.SHA256, sum[:], signature) == nil {
			return nil
		}
	default:
		return ErrUnsupported
	}
	return ErrInvalid
}
