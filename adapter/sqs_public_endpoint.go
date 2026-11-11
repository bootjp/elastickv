package adapter

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

var errInvalidSQSPublicEndpoint = errors.New("SQS public endpoint must be an HTTP(S) origin without credentials, a path, query, or fragment")

// WithSQSPublicEndpoint sets the externally reachable origin used in queue URLs.
// An empty value preserves request-derived URLs. Forwarding headers are never trusted.
func WithSQSPublicEndpoint(raw string) (SQSServerOption, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return func(s *SQSServer) { s.publicEndpoint = "" }, nil
	}
	if strings.Contains(raw, "#") {
		return nil, errInvalidSQSPublicEndpoint
	}
	endpoint, err := url.Parse(raw)
	if err != nil {
		return nil, errors.Wrap(errInvalidSQSPublicEndpoint, "parse endpoint")
	}
	if err := validateSQSPublicEndpoint(endpoint); err != nil {
		return nil, err
	}
	origin := endpoint.Scheme + "://" + endpoint.Host
	return func(s *SQSServer) { s.publicEndpoint = origin }, nil
}

func validateSQSPublicEndpoint(endpoint *url.URL) error {
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return errInvalidSQSPublicEndpoint
	}
	if endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Opaque != "" {
		return errInvalidSQSPublicEndpoint
	}
	if endpoint.Path != "" && endpoint.Path != "/" {
		return errInvalidSQSPublicEndpoint
	}
	if err := validateSQSPublicEndpointPort(endpoint.Port()); err != nil {
		return err
	}
	if hasSQSPublicEndpointSuffix(endpoint) {
		return errInvalidSQSPublicEndpoint
	}
	return nil
}

func hasSQSPublicEndpointSuffix(endpoint *url.URL) bool {
	return endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawFragment != ""
}

func validateSQSPublicEndpointPort(port string) error {
	if port == "" {
		return nil
	}
	value, err := strconv.ParseUint(port, 10, 16)
	if err != nil || value == 0 {
		return errors.Wrap(errInvalidSQSPublicEndpoint, "port must be between 1 and 65535")
	}
	return nil
}
