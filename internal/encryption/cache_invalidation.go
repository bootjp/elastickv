package encryption

// SetBeforeEncryptionChange wires an optional memory-only paired authority fence
// under startup exclusivity, before publishing any permission. It runs BEFORE
// actual storage key/envelope/registration gate writes, including registration
// goroutines outside native apply. Durable sidecar/registry mutations additionally
// need native pre-apply invalidation; this hook alone does not fence those writes.
func (c *StateCache) SetBeforeEncryptionChange(invalidate func() func()) {
	if invalidate == nil {
		c.beforeEncryptionChange.Store(nil)
		return
	}
	c.beforeEncryptionChange.Store(&invalidate)
}

func (c *StateCache) beginEncryptionChange() func() {
	if callback := c.beforeEncryptionChange.Load(); callback != nil {
		end := (*callback)()
		if end == nil {
			panic("encryption cache mutation callback returned no end fence")
		}
		return end
	}
	return func() {}
}
