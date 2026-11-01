package encryption

// NonceIdentity observes the installed factory without consuming a nonce.
// The identity is fixed at construction; exhaustion is latched permanently.
func (f *DeterministicNonceFactory) NonceIdentity() (nodeID, epoch uint16, exhausted bool) {
	return f.nodeID, f.localEpoch, f.exhausted.Load()
}

// HasKey observes the cipher's actual keystore, not the sidecar or StateCache.
func (c *Cipher) HasKey(id uint32) bool {
	return c != nil && c.keystore != nil && c.keystore.Has(id)
}
