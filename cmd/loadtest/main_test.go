package main

import (
	"testing"

	"github.com/bootjp/elastickv/internal/loadtest"
	"github.com/stretchr/testify/require"
)

func TestRatesAndTransportRejectInvalidConfiguration(t *testing.T) {
	rates, err := parseRates("10, 50,200")
	require.NoError(t, err)
	require.Equal(t, []int{10, 50, 200}, rates)
	for _, text := range []string{"", "0", "-1", "5,wat"} {
		_, err = parseRates(text)
		require.Error(t, err)
	}
	_, err = dialOptions(true, "ca.pem", "")
	require.Error(t, err)
	_, err = dialOptions(true, "", "token")
	require.Error(t, err)
	opts, err := dialOptions(true, "", "")
	require.NoError(t, err)
	require.NotEmpty(t, opts)
}

func TestAuditRejectsUnexpectedAttempts(t *testing.T) {
	audit := loadtest.Metric{Attempts: 4, Success: 3, Errors: map[string]int{"value_mismatch": 1}}
	require.Error(t, validateResult(nil, audit, 3, 50))
	audit.Attempts = 3
	audit.Errors = nil
	require.NoError(t, validateResult(nil, audit, 3, 50))
}
