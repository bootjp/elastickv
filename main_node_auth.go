package main

import (
	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

type nodeGRPCAuthConfig struct {
	adminToken     string
	peerToken      string
	migrationToken string
	adminEnabled   bool
}

func loadNodeGRPCAuth(tokenPath string, insecureNoAuth bool, peerTokenPath, migrationTokenPath string) (nodeGRPCAuthConfig, error) {
	if tokenPath != "" && insecureNoAuth {
		return nodeGRPCAuthConfig{}, errors.New("--adminInsecureNoAuth and --adminTokenFile are mutually exclusive")
	}
	var cfg nodeGRPCAuthConfig
	cfg.adminEnabled = tokenPath != "" || insecureNoAuth
	if tokenPath != "" {
		loaded, err := loadAdminTokenFile(tokenPath)
		if err != nil {
			return nodeGRPCAuthConfig{}, err
		}
		cfg.adminToken = loaded
	}
	if peerTokenPath != "" {
		loaded, err := loadS3BlobPeerTokenFile(peerTokenPath)
		if err != nil {
			return nodeGRPCAuthConfig{}, err
		}
		cfg.peerToken = loaded
	}
	if err := validateNodeGRPCPeerToken(cfg.adminToken, cfg.peerToken); err != nil {
		return nodeGRPCAuthConfig{}, err
	}
	migrationToken, err := loadSplitMigrationToken(migrationTokenPath, insecureNoAuth, cfg.adminToken, cfg.peerToken)
	if err != nil {
		return nodeGRPCAuthConfig{}, err
	}
	cfg.migrationToken = migrationToken
	return cfg, nil
}

func validateMigrationOpcodeAuth(token string, insecureNoAuth bool) error {
	if token != "" || insecureNoAuth {
		return nil
	}
	if adapter.MigrationImportOpcodeEnabledFromEnv() || adapter.MigrationPromoteOpcodeEnabledFromEnv() || adapter.MigrationCleanupOpcodeEnabledFromEnv() {
		return errors.New("migration opcodes require --splitMigrationTokenFile or explicit test-only --adminInsecureNoAuth")
	}
	return nil
}

func validateMigrationStartupAuth() error {
	auth, err := loadNodeGRPCAuth(*adminTokenFile, *adminInsecureNoAuth, *s3BlobPeerTokenFile, *splitMigrationTokenFile)
	if err != nil {
		return err
	}
	return validateMigrationPeerSecurity(auth.migrationToken, *adminInsecureNoAuth, *splitMigrationPeerAuthEnforced, *grpcTLSCertFile != "" && *grpcTLSKeyFile != "")
}

func configureNodeGRPCTransport() error {
	if err := internalutil.ConfigureGRPCTLS(*grpcTLSCAFile, *grpcTLSCertFile, *grpcTLSKeyFile); err != nil {
		return errors.Wrap(err, "configure peer gRPC transport")
	}
	return validateMigrationStartupAuth()
}

func validateMigrationPeerSecurity(token string, insecureNoAuth, enforced, tlsEnabled bool) error {
	if insecureNoAuth {
		return nil
	}
	if enforced && token == "" {
		return errors.New("--splitMigrationPeerAuthEnforced requires --splitMigrationTokenFile")
	}
	if token != "" && !tlsEnabled {
		return errors.New("--splitMigrationTokenFile requires gRPC TLS certificate and key configuration")
	}
	return nil
}

func validateNodeGRPCPeerToken(adminToken, peerToken string) error {
	if adminToken != "" && peerToken == adminToken {
		return errors.New("S3 blob peer token must differ from the read-only admin token")
	}
	return nil
}

func loadSplitMigrationToken(path string, insecureNoAuth bool, adminToken, peerToken string) (string, error) {
	if path == "" {
		return "", validateMigrationOpcodeAuth("", insecureNoAuth)
	}
	if insecureNoAuth {
		return "", errors.New("--adminInsecureNoAuth and --splitMigrationTokenFile are mutually exclusive")
	}
	const maxMigrationTokenBytes = 4 << 10
	token, err := internalutil.LoadBearerTokenFile(path, maxMigrationTokenBytes, "migration token")
	if err != nil {
		return "", errors.WithStack(err)
	}
	if token == adminToken || token == peerToken {
		return "", errors.New("migration token must differ from Admin and S3 peer tokens")
	}
	return token, nil
}

func appendGRPCBearerInterceptors(
	icept *adminGRPCInterceptors,
	unary grpc.UnaryServerInterceptor,
	stream grpc.StreamServerInterceptor,
) {
	if unary != nil {
		icept.unary = append(icept.unary, unary)
	}
	if stream != nil {
		icept.stream = append(icept.stream, stream)
	}
}
