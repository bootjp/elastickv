// Command elastickv-snapshot-archive packs and unpacks Phase 0 logical dump
// trees as tar or tar+zstd streams.
package main

import (
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/bootjp/elastickv/internal/backup"
	"github.com/cockroachdb/errors"
)

const (
	exitSuccess           = 0
	exitUserErr           = 1
	exitDataErr           = 2
	archiveOutputFilePerm = 0o600
)

var errArchiveOutputInsideInput = errors.New("snapshot archive: output path is inside input tree")

type config struct {
	mode        string
	inputPath   string
	outputPath  string
	compression string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	code, err := run(os.Args[1:], logger)
	if err != nil {
		logger.Error("elastickv-snapshot-archive", "err", err)
	}
	os.Exit(code)
}

func run(argv []string, logger *slog.Logger) (int, error) {
	cfg, err := parseFlags(argv)
	if err != nil {
		return exitUserErr, err
	}
	if err := runArchive(cfg, logger); err != nil {
		return classifyError(err), err
	}
	return exitSuccess, nil
}

func classifyError(err error) int {
	switch {
	case errors.Is(err, backup.ErrInvalidManifest),
		errors.Is(err, backup.ErrUnsupportedFormatVersion),
		errors.Is(err, backup.ErrChecksumMismatch),
		errors.Is(err, backup.ErrChecksumsMalformedLine),
		errors.Is(err, backup.ErrChecksumsEmpty),
		errors.Is(err, backup.ErrChecksumsPathTraversal),
		errors.Is(err, backup.ErrChecksumsSymlinkEscape),
		errors.Is(err, backup.ErrArchivePathUnsafe),
		errors.Is(err, backup.ErrArchiveNonRegular),
		errors.Is(err, backup.ErrArchiveUnchecksummedFile),
		errors.Is(err, backup.ErrArchiveBudgetExceeded):
		return exitDataErr
	default:
		return exitUserErr
	}
}

func parseFlags(argv []string) (*config, error) {
	if len(argv) == 0 {
		return nil, errors.New("subcommand required: pack or unpack")
	}
	cfg := &config{mode: argv[0], compression: backup.ArchiveCompressionZstd}
	fs := flag.NewFlagSet("elastickv-snapshot-archive "+cfg.mode, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.inputPath, "input", "", "Input dump root for pack, or archive file for unpack (use - for stdin on unpack)")
	fs.StringVar(&cfg.outputPath, "output", "", "Output archive file for pack (use - for stdout), or output dump root for unpack")
	fs.StringVar(&cfg.compression, "compression", backup.ArchiveCompressionZstd, "Compression: zstd or none")
	if err := fs.Parse(argv[1:]); err != nil {
		return nil, errors.WithStack(err)
	}
	if cfg.inputPath == "" {
		return nil, errors.New("--input is required")
	}
	if cfg.outputPath == "" {
		return nil, errors.New("--output is required")
	}
	switch cfg.mode {
	case "pack", "unpack":
	default:
		return nil, errors.Errorf("unknown subcommand %q", cfg.mode)
	}
	if cfg.compression != backup.ArchiveCompressionZstd && cfg.compression != backup.ArchiveCompressionNone {
		return nil, errors.Wrapf(backup.ErrArchiveCompressionUnsupported, "%q", cfg.compression)
	}
	return cfg, nil
}

func runArchive(cfg *config, logger *slog.Logger) error {
	switch cfg.mode {
	case "pack":
		return runPack(cfg, logger)
	case "unpack":
		return runUnpack(cfg, logger)
	default:
		return errors.Errorf("unknown subcommand %q", cfg.mode)
	}
}

func runPack(cfg *config, logger *slog.Logger) error {
	if err := rejectArchiveOutputInsideInput(cfg.inputPath, cfg.outputPath); err != nil {
		return err
	}
	out, closeFn, err := openArchiveOutput(cfg.outputPath)
	if err != nil {
		return err
	}
	if err := backup.PackDumpTree(cfg.inputPath, out, cfg.compression); err != nil {
		_ = closeFn()
		return errors.Wrap(err, "pack snapshot dump tree")
	}
	if err := closeFn(); err != nil {
		return err
	}
	logger.Info("snapshot dump archive written", "input", cfg.inputPath, "output", cfg.outputPath, "compression", cfg.compression)
	return nil
}

func rejectArchiveOutputInsideInput(inputPath string, outputPath string) error {
	if outputPath == "-" {
		return nil
	}
	inputAbs, err := filepath.Abs(inputPath)
	if err != nil {
		return errors.WithStack(err)
	}
	outputAbs, err := filepath.Abs(outputPath)
	if err != nil {
		return errors.WithStack(err)
	}
	rel, err := filepath.Rel(inputAbs, outputAbs)
	if err != nil {
		return errors.WithStack(err)
	}
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)) {
		return errors.Wrapf(errArchiveOutputInsideInput, "%s under %s", outputPath, inputPath)
	}
	return nil
}

func runUnpack(cfg *config, logger *slog.Logger) error {
	in, closeFn, err := openArchiveInput(cfg.inputPath)
	if err != nil {
		return err
	}
	if err := backup.UnpackDumpTree(in, cfg.outputPath, cfg.compression); err != nil {
		_ = closeFn()
		return errors.Wrap(err, "unpack snapshot dump tree")
	}
	if err := closeFn(); err != nil {
		return err
	}
	logger.Info("snapshot dump archive unpacked", "input", cfg.inputPath, "output", cfg.outputPath, "compression", cfg.compression)
	return nil
}

func openArchiveOutput(path string) (io.Writer, func() error, error) {
	if path == "-" {
		return os.Stdout, func() error { return nil }, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, archiveOutputFilePerm) //nolint:gosec // operator-supplied output path
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return f, func() error {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return errors.WithStack(err)
		}
		return errors.WithStack(f.Close())
	}, nil
}

func openArchiveInput(path string) (io.Reader, func() error, error) {
	if path == "-" {
		return os.Stdin, func() error { return nil }, nil
	}
	f, err := os.Open(path) //nolint:gosec // operator-supplied input path
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return f, func() error { return errors.WithStack(f.Close()) }, nil
}
