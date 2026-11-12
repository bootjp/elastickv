package clusterbootstrap

import (
	"slices"

	"github.com/bootjp/elastickv/internal/nodeauth"
)

// Wrap only the original service-auth interceptors, before the public/runtime
// gate is appended. The startup exception must never skip A.Enter itself.
func BootstrapServiceAuthOptions(options nodeauth.AdminGRPCInterceptors, domain *BootstrapDomain) nodeauth.AdminGRPCInterceptors {
	if domain == nil {
		return options
	}
	options.Unary = slices.Clone(options.Unary)
	options.Stream = slices.Clone(options.Stream)
	for i, original := range options.Unary {
		options.Unary[i] = BootstrapStartupAuthException(domain, original)
	}
	return options
}
