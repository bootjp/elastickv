package monitoring

import (
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
)

// MetricsAddressRequiresToken reports whether the metrics endpoint is exposed beyond loopback.
func MetricsAddressRequiresToken(addr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return true
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "0.0.0.0" || host == "::" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsLoopback()
}

// ProtectHandler wraps a metrics handler with optional bearer-token authentication.
func ProtectHandler(handler http.Handler, bearerToken string) http.Handler {
	if handler == nil {
		return nil
	}
	token := strings.TrimSpace(bearerToken)
	if token == "" {
		return handler
	}
	expected := "Bearer " + token
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(r.Header.Get("Authorization"))), []byte(expected)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	})
}
