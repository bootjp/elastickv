package kv

// These hooks are installed before receivers/workers start. Each wrapper owns
// its normal concurrent cache; none borrows the serialized startup-control lane.
func (s *ShardStore) SetInternalGRPCDialOptions(options GRPCDialOptionsForAddress) error {
	return s.connCache.SetDialOptions(options)
}

func (s *ShardStore) InternalGRPCDialOptions() GRPCDialOptionsForAddress {
	if s == nil {
		return nil
	}
	s.connCache.mu.Lock()
	defer s.connCache.mu.Unlock()
	return s.connCache.dialOptions
}

func (p *LeaderProxy) SetInternalGRPCDialOptions(options GRPCDialOptionsForAddress) error {
	return p.connCache.SetDialOptions(options)
}

func (c *Coordinate) SetInternalGRPCDialOptions(options GRPCDialOptionsForAddress) error {
	return c.connCache.SetDialOptions(options)
}

func (s *LeaderRoutedStore) SetInternalGRPCDialOptions(options GRPCDialOptionsForAddress) error {
	return s.connCache.SetDialOptions(options)
}

func WithTSOInternalGRPCDialOptions(options GRPCDialOptionsForAddress) LeaderRoutedTSOAllocatorOption {
	return func(a *LeaderRoutedTSOAllocator) { a.connCache.dialOptions = options }
}
