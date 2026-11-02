package main

import "slices"

// Wrap only the original service-auth interceptors, before the public/runtime
// gate is appended. The startup exception must never skip A.Enter itself.
func bootstrapServiceAuthOptions(options adminGRPCInterceptors, domain *bootstrapDomain) adminGRPCInterceptors {
	if domain == nil {
		return options
	}
	options.unary = slices.Clone(options.unary)
	options.stream = slices.Clone(options.stream)
	for i, original := range options.unary {
		options.unary[i] = bootstrapStartupAuthException(domain, original)
	}
	return options
}
