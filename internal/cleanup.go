package internal

// CleanupStack stores cleanup callbacks and runs them in LIFO order.
type CleanupStack struct {
	funcs []func()
}

// Add registers a cleanup callback.
func (c *CleanupStack) Add(fn func()) {
	if fn == nil {
		return
	}
	c.funcs = append(c.funcs, fn)
}

// Release discards all registered callbacks.
func (c *CleanupStack) Release() {
	c.funcs = nil
}

// Run executes registered callbacks in reverse registration order.
func (c *CleanupStack) Run() {
	for i := len(c.funcs) - 1; i >= 0; i-- {
		c.funcs[i]()
	}
}
