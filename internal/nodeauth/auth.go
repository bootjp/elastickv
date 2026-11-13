package nodeauth

import (
	"flag"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// Flags configures authentication between nodes and toward the
// node-local Admin gRPC service: bearer-token files, the development-only
// insecure mode, the split-migration rollout switch, and peer gRPC TLS.
// They are independent of the admin HTTP listener flags; operators can
// enable either auth path, or both.
type Flags struct {
	AdminTokenFile                 string
	AdminInsecureNoAuth            bool
	SplitMigrationTokenFile        string
	SplitMigrationPeerAuthEnforced bool
	GRPCTLSCAFile                  string
	GRPCTLSCertFile                string
	GRPCTLSKeyFile                 string
	S3BlobPeerTokenFile            string
}

func (f *Flags) Register(fs *flag.FlagSet) {
	fs.StringVar(&f.AdminTokenFile, "adminTokenFile", "", "Path to the read-only Admin bearer token; does not authorize split migration")
	fs.BoolVar(&f.AdminInsecureNoAuth, "adminInsecureNoAuth", false, "Register the Admin gRPC service without bearer-token authentication; development only")
	fs.StringVar(&f.SplitMigrationTokenFile, "splitMigrationTokenFile", "", "Path to a dedicated cluster-shared migration controller/node token, distinct from Admin and S3 peer tokens")
	fs.BoolVar(&f.SplitMigrationPeerAuthEnforced, "splitMigrationPeerAuthEnforced", false, "Require TLS peer Forward credentials and enable migration only after every node sends the migration token")
	fs.StringVar(&f.GRPCTLSCAFile, "grpcTLSCAFile", "", "CA certificates for verifying peer gRPC TLS; requires grpcTLSCertFile and grpcTLSKeyFile")
	fs.StringVar(&f.GRPCTLSCertFile, "grpcTLSCertFile", "", "PEM certificate enabling TLS on group gRPC listeners and peer clients")
	fs.StringVar(&f.GRPCTLSKeyFile, "grpcTLSKeyFile", "", "PEM private key for group gRPC TLS")
	fs.StringVar(&f.S3BlobPeerTokenFile, "s3BlobPeerTokenFile", "", "Path to the cluster-shared bearer token for peer S3 chunkblob RPCs")
}

// ServiceTokenFiles lists the token files a protected bootstrap startup
// token must not reuse.
func (f Flags) ServiceTokenFiles() []string {
	return []string{f.AdminTokenFile, f.SplitMigrationTokenFile, f.S3BlobPeerTokenFile}
}

func (f Flags) LoadGRPCAuth() (GRPCAuthConfig, error) {
	return LoadNodeGRPCAuth(f.AdminTokenFile, f.AdminInsecureNoAuth, f.S3BlobPeerTokenFile, f.SplitMigrationTokenFile)
}

type GRPCAuthConfig struct {
	AdminToken     string
	PeerToken      string
	MigrationToken string
	AdminEnabled   bool
}

func LoadNodeGRPCAuth(tokenPath string, insecureNoAuth bool, peerTokenPath, migrationTokenPath string) (GRPCAuthConfig, error) {
	if tokenPath != "" && insecureNoAuth {
		return GRPCAuthConfig{}, errors.New("--adminInsecureNoAuth and --adminTokenFile are mutually exclusive")
	}
	var cfg GRPCAuthConfig
	cfg.AdminEnabled = tokenPath != "" || insecureNoAuth
	if tokenPath != "" {
		loaded, err := LoadAdminTokenFile(tokenPath)
		if err != nil {
			return GRPCAuthConfig{}, err
		}
		cfg.AdminToken = loaded
	}
	if peerTokenPath != "" {
		loaded, err := LoadS3BlobPeerTokenFile(peerTokenPath)
		if err != nil {
			return GRPCAuthConfig{}, err
		}
		cfg.PeerToken = loaded
	}
	if err := validateNodeGRPCPeerToken(cfg.AdminToken, cfg.PeerToken); err != nil {
		return GRPCAuthConfig{}, err
	}
	migrationToken, err := loadSplitMigrationToken(migrationTokenPath, insecureNoAuth, cfg.AdminToken, cfg.PeerToken)
	if err != nil {
		return GRPCAuthConfig{}, err
	}
	cfg.MigrationToken = migrationToken
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

func validateMigrationStartupAuth(f Flags) error {
	auth, err := f.LoadGRPCAuth()
	if err != nil {
		return err
	}
	return ValidateMigrationPeerSecurity(auth.MigrationToken, f.AdminInsecureNoAuth, f.SplitMigrationPeerAuthEnforced, f.GRPCTLSCertFile != "" && f.GRPCTLSKeyFile != "")
}

func ConfigureGRPCTransport(f Flags) error {
	if err := internalutil.ConfigureGRPCTLS(f.GRPCTLSCAFile, f.GRPCTLSCertFile, f.GRPCTLSKeyFile); err != nil {
		return errors.Wrap(err, "configure peer gRPC transport")
	}
	return validateMigrationStartupAuth(f)
}

func ValidateMigrationPeerSecurity(token string, insecureNoAuth, enforced, tlsEnabled bool) error {
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

func AppendGRPCBearerInterceptors(
	icept *AdminGRPCInterceptors,
	unary grpc.UnaryServerInterceptor,
	stream grpc.StreamServerInterceptor,
) {
	if unary != nil {
		icept.Unary = append(icept.Unary, unary)
	}
	if stream != nil {
		icept.Stream = append(icept.Stream, stream)
	}
}
