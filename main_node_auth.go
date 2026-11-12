package main

import (
	"flag"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// nodeAuthFlags configures authentication between nodes and toward the
// node-local Admin gRPC service: bearer-token files, the development-only
// insecure mode, the split-migration rollout switch, and peer gRPC TLS.
// They are independent of the admin HTTP listener flags; operators can
// enable either auth path, or both.
type nodeAuthFlags struct {
	adminTokenFile                 string
	adminInsecureNoAuth            bool
	splitMigrationTokenFile        string
	splitMigrationPeerAuthEnforced bool
	grpcTLSCAFile                  string
	grpcTLSCertFile                string
	grpcTLSKeyFile                 string
	s3BlobPeerTokenFile            string
}

func (f *nodeAuthFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.adminTokenFile, "adminTokenFile", "", "Path to the read-only Admin bearer token; does not authorize split migration")
	fs.BoolVar(&f.adminInsecureNoAuth, "adminInsecureNoAuth", false, "Register the Admin gRPC service without bearer-token authentication; development only")
	fs.StringVar(&f.splitMigrationTokenFile, "splitMigrationTokenFile", "", "Path to a dedicated cluster-shared migration controller/node token, distinct from Admin and S3 peer tokens")
	fs.BoolVar(&f.splitMigrationPeerAuthEnforced, "splitMigrationPeerAuthEnforced", false, "Require TLS peer Forward credentials and enable migration only after every node sends the migration token")
	fs.StringVar(&f.grpcTLSCAFile, "grpcTLSCAFile", "", "CA certificates for verifying peer gRPC TLS; requires grpcTLSCertFile and grpcTLSKeyFile")
	fs.StringVar(&f.grpcTLSCertFile, "grpcTLSCertFile", "", "PEM certificate enabling TLS on group gRPC listeners and peer clients")
	fs.StringVar(&f.grpcTLSKeyFile, "grpcTLSKeyFile", "", "PEM private key for group gRPC TLS")
	fs.StringVar(&f.s3BlobPeerTokenFile, "s3BlobPeerTokenFile", "", "Path to the cluster-shared bearer token for peer S3 chunkblob RPCs")
}

// serviceTokenFiles lists the token files a protected bootstrap startup
// token must not reuse.
func (f nodeAuthFlags) serviceTokenFiles() []string {
	return []string{f.adminTokenFile, f.splitMigrationTokenFile, f.s3BlobPeerTokenFile}
}

func (f nodeAuthFlags) loadGRPCAuth() (nodeGRPCAuthConfig, error) {
	return loadNodeGRPCAuth(f.adminTokenFile, f.adminInsecureNoAuth, f.s3BlobPeerTokenFile, f.splitMigrationTokenFile)
}

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

func validateMigrationStartupAuth(f nodeAuthFlags) error {
	auth, err := f.loadGRPCAuth()
	if err != nil {
		return err
	}
	return validateMigrationPeerSecurity(auth.migrationToken, f.adminInsecureNoAuth, f.splitMigrationPeerAuthEnforced, f.grpcTLSCertFile != "" && f.grpcTLSKeyFile != "")
}

func configureNodeGRPCTransport(f nodeAuthFlags) error {
	if err := internalutil.ConfigureGRPCTLS(f.grpcTLSCAFile, f.grpcTLSCertFile, f.grpcTLSKeyFile); err != nil {
		return errors.Wrap(err, "configure peer gRPC transport")
	}
	return validateMigrationStartupAuth(f)
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
