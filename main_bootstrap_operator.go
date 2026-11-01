package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/signal"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
)

func dispatchBootstrapOperator(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != "bootstrap" {
		return false, nil
	}
	if handled, err := dispatchBootstrapObservationOrArtifact(args[1:], output); handled {
		return true, err
	}
	request, err := parseBootstrapBaselineCLI(args[1:])
	if err != nil {
		return true, err
	}
	expected, err := readBootstrapOperatorInput(request.ExpectedConfigPath)
	if err != nil {
		return true, err
	}
	manifest, err := readBootstrapOperatorInput(request.ManifestPath)
	if err != nil {
		return true, err
	}
	summary, err := bootstrapBaselineDryRun(request, expected, manifest)
	if err != nil {
		return true, err
	}
	if request.Command != bootstrapBaselineInspect {
		return true, executeBootstrapMigrationInputs(request, expected, manifest, output)
	}
	if err := verifyBootstrapInspectionInputs(request, expected, manifest); err != nil {
		return true, err
	}
	summary.NativeStateVerified = true
	return true, errors.WithStack(json.NewEncoder(output).Encode(summary))
}

func verifyBootstrapInspectionInputs(request bootstrapBaselineCLIRequest, expectedRaw, manifestRaw []byte) error {
	expected, err := parseBootstrapBaselineExpectedConfig(expectedRaw, request.OperationID)
	if err != nil {
		return err
	}
	manifest, err := raftbootstrap.ParseBaselineManifest(manifestRaw)
	if err != nil {
		return errors.WithStack(err)
	}
	return inspectBootstrapNativeInput(context.Background(), request, expected, manifest)
}

func executeBootstrapMigrationInputs(request bootstrapBaselineCLIRequest, expectedRaw, manifestRaw []byte, output io.Writer) error {
	expected, err := parseBootstrapBaselineExpectedConfig(expectedRaw, request.OperationID)
	if err != nil {
		return err
	}
	manifest, err := raftbootstrap.ParseBaselineManifest(manifestRaw)
	if err != nil {
		return errors.WithStack(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	migration, err := executeBootstrapMigration(ctx, request, expected, manifest)
	if err != nil {
		return err
	}
	return errors.WithStack(json.NewEncoder(output).Encode(struct {
		Command                  string                        `json:"command"`
		Migration                etcd.OfflineBaselineMigration `json:"migration"`
		PolicyActivationVerified bool                          `json:"policy_activation_verified"`
	}{Command: request.Command, Migration: migration}))
}

func readBootstrapOperatorInput(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open baseline input")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat baseline input")
	}
	if !info.Mode().IsRegular() || info.Size() > raftbootstrap.MaxFileSize {
		return nil, errors.New("baseline input must be a regular file within 16 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(f, raftbootstrap.MaxFileSize+1))
	if err != nil {
		return nil, errors.Wrap(err, "read baseline input")
	}
	if len(data) > raftbootstrap.MaxFileSize {
		return nil, errors.New("baseline input exceeds 16 MiB")
	}
	return data, nil
}
