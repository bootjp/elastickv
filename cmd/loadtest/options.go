package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"time"

	"github.com/bootjp/elastickv/internal/loadtest"
)

type options struct {
	Config           loadtest.Config
	Addresses        string
	Plain            bool
	CA, TokenFile    string
	AuditReadyFile   string
	AuditWaitTimeout time.Duration
}

func parseOptions(args []string) (options, error) {
	f := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	addresses := f.String("addresses", "127.0.0.1:50051", "Comma-separated RawKV ingress endpoints")
	rates := f.String("rates", "10,25,50,100,250", "Offered logical operations/s per stage")
	duration := f.Duration("stage-duration", loadtest.DefaultStageDuration, "Scheduling duration of each stage; drain is additional")
	concurrency := f.Int("concurrency", loadtest.DefaultConcurrency, "Maximum concurrent logical operations")
	reads := f.Int("read-percent", loadtest.DefaultReadPercent, "Read share of the deterministic operation schedule")
	keys := f.Int("seed-keys", loadtest.DefaultSeedKeys, "Immutable seeded keys used by measured reads")
	size := f.Int("value-bytes", loadtest.DefaultValueBytes, "Value bytes, including operation identity")
	maxRequests := f.Int("max-requests", loadtest.DefaultMaxRequests, "Maximum scheduled requests and in-memory audit/sample records")
	prefix := f.String("prefix", "", "Unused run prefix; empty generates a random run ID")
	timeout := f.Duration("request-timeout", loadtest.DefaultRequestTimeout, "Deadline per RPC; mutations are never retried")
	verify := f.Bool("verify-writes", true, "Read back every successful write immediately (reported separately)")
	plain := f.Bool("plaintext", false, "Explicitly use plaintext instead of TLS")
	ca := f.String("ca-file", "", "Trusted CA for TLS; empty uses system roots")
	tokenFile := f.String("token-file", "", "Optional file containing a gRPC bearer token (TLS only)")
	auditReady := f.String("audit-ready-file", "", "Optional absent marker created by a Split controller after Done; audit waits for it")
	auditTimeout := f.Duration("audit-wait-timeout", defaultAuditWaitTimeout, "Maximum wait for the external audit-ready marker")
	if err := f.Parse(args); err != nil {
		return options{}, err
	}
	if f.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected arguments: %v", f.Args())
	}
	parsed, err := parseRates(*rates)
	if err != nil {
		return options{}, err
	}
	if *prefix == "" {
		var nonce [12]byte
		if _, err = rand.Read(nonce[:]); err != nil {
			return options{}, err
		}
		*prefix = hex.EncodeToString(nonce[:])
	}
	config := loadtest.Config{Rates: parsed, StageDuration: *duration, Concurrency: *concurrency, ReadPercent: *reads,
		Keys: *keys, ValueBytes: *size, MaxRequests: *maxRequests, Prefix: *prefix, RequestTimeout: *timeout, VerifyWrites: *verify}
	return options{Config: config, Addresses: *addresses, Plain: *plain, CA: *ca, TokenFile: *tokenFile, AuditReadyFile: *auditReady, AuditWaitTimeout: *auditTimeout}, nil
}
