package clusterbootstrap

import (
	"encoding/json"
	"flag"
	"io"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
)

const bootstrapMarkerUpgradeCommand = "upgrade-marker"

type BootstrapMarkerUpgradeRequest struct {
	DataDir  string
	Identity raftbootstrap.Identity
	MemberID uint64
	DryRun   bool
}

type BootstrapMarkerUpgradeResult struct {
	Command       string `json:"command"`
	DataDir       string `json:"raft_data_dir"`
	DryRun        bool   `json:"dry_run"`
	AlreadyFenced bool   `json:"already_fenced"`
	Changed       bool   `json:"changed"`
}

func ParseBootstrapMarkerUpgradeCLI(args []string) (BootstrapMarkerUpgradeRequest, error) {
	if err := bootstrapBaselineArgsBound(args); err != nil {
		return BootstrapMarkerUpgradeRequest{}, err
	}
	if args[0] != bootstrapMarkerUpgradeCommand {
		return BootstrapMarkerUpgradeRequest{}, errors.New("unsupported marker upgrade command")
	}
	request, cluster, group, member, err := parseMarkerUpgradeFlags(args[1:])
	if err != nil {
		return BootstrapMarkerUpgradeRequest{}, err
	}
	return parseMarkerUpgradeIdentity(request, cluster, group, member)
}

func parseMarkerUpgradeFlags(args []string) (BootstrapMarkerUpgradeRequest, string, string, string, error) {
	var request BootstrapMarkerUpgradeRequest
	fs := flag.NewFlagSet("bootstrap upgrade-marker", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cluster, group, member string
	values := map[string]*string{
		"raft-data-dir": &request.DataDir, "cluster-uuid": &cluster,
		"group-id": &group, "member-id": &member,
	}
	seen := make(map[string]bool, len(values))
	for name, target := range values {
		fs.Func(name, "required marker upgrade input", func(value string) error {
			if seen[name] {
				return errors.New("duplicate marker upgrade option")
			}
			seen[name] = true
			*target = value
			return nil
		})
	}
	fs.BoolVar(&request.DryRun, "dry-run", false, "inspect without changing the marker")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || len(seen) != len(values) {
		return BootstrapMarkerUpgradeRequest{}, "", "", "", errors.New("marker upgrade requires four named inputs and no positional arguments")
	}
	return request, cluster, group, member, nil
}

func parseMarkerUpgradeIdentity(request BootstrapMarkerUpgradeRequest, cluster, group, member string) (BootstrapMarkerUpgradeRequest, error) {
	if !bootstrapBaselineValidPath(request.DataDir) {
		return BootstrapMarkerUpgradeRequest{}, errors.New("invalid raft data directory")
	}
	var err error
	request.Identity.Cluster, err = raftbootstrap.ParseUUID(cluster)
	if err != nil || request.Identity.Cluster == (raftbootstrap.UUID{}) {
		return BootstrapMarkerUpgradeRequest{}, errors.New("invalid cluster UUID")
	}
	request.Identity.Group, err = bootstrapBaselineUint(group)
	if err != nil {
		return BootstrapMarkerUpgradeRequest{}, errors.New("invalid group ID")
	}
	request.MemberID, err = bootstrapBaselineUint(member)
	if err != nil || request.MemberID == 0 {
		return BootstrapMarkerUpgradeRequest{}, errors.New("invalid member ID")
	}
	return request, nil
}

func ExecuteBootstrapMarkerUpgrade(request BootstrapMarkerUpgradeRequest) (_ BootstrapMarkerUpgradeResult, err error) {
	result := BootstrapMarkerUpgradeResult{Command: bootstrapMarkerUpgradeCommand, DataDir: request.DataDir, DryRun: request.DryRun}
	if !bootstrapBaselineValidPath(request.DataDir) || request.Identity.Cluster == (raftbootstrap.UUID{}) || request.MemberID == 0 {
		return BootstrapMarkerUpgradeResult{}, errors.New("invalid marker upgrade request")
	}
	ownership, err := etcd.AcquireOfflineBaselineOwnership(request.DataDir)
	if err != nil {
		return BootstrapMarkerUpgradeResult{}, errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, ownership.Close()) }()
	files := raftbootstrap.NewStore(request.DataDir)
	if err := validateMarkerUpgradeFiles(files, request); err != nil {
		return BootstrapMarkerUpgradeResult{}, err
	}
	return applyMarkerUpgrade(files, request, result)
}

func validateMarkerUpgradeFiles(files *raftbootstrap.Store, request BootstrapMarkerUpgradeRequest) error {
	intent, present, err := files.LoadIntent()
	if err != nil || !present || intent.Identity != request.Identity || !intentContainsMember(intent, request.MemberID) {
		return errors.Join(raftbootstrap.ErrInconsistent, err)
	}
	return verifyMarkerUpgradePolicy(files, intent)
}

func applyMarkerUpgrade(files *raftbootstrap.Store, request BootstrapMarkerUpgradeRequest, result BootstrapMarkerUpgradeResult) (BootstrapMarkerUpgradeResult, error) {
	var err error
	result.AlreadyFenced, err = files.InspectRequiredMarkerFence(request.Identity)
	if err != nil {
		return result, errors.WithStack(err)
	}
	if request.DryRun {
		return result, nil
	}
	result.Changed, err = files.UpgradeRequiredMarkerFence(request.Identity)
	if err != nil {
		return result, errors.WithStack(err)
	}
	fenced, err := files.InspectRequiredMarkerFence(request.Identity)
	if err != nil || !fenced {
		return result, errors.Join(raftbootstrap.ErrInconsistent, err)
	}
	return result, nil
}

func intentContainsMember(intent raftbootstrap.Intent, memberID uint64) bool {
	for _, member := range intent.Baseline.Members {
		if member.ID == memberID {
			return true
		}
	}
	return false
}

func verifyMarkerUpgradePolicy(files *raftbootstrap.Store, intent raftbootstrap.Intent) error {
	policy, present, err := files.LoadPolicy()
	if err != nil {
		return errors.WithStack(err)
	}
	if !present {
		return nil
	}
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	if err != nil {
		return errors.WithStack(err)
	}
	if policy.Identity != intent.Identity || policy.BaselineDigest != digest {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}

func dispatchBootstrapMarkerUpgrade(args []string, output io.Writer) error {
	request, err := ParseBootstrapMarkerUpgradeCLI(args)
	if err != nil {
		return err
	}
	result, err := ExecuteBootstrapMarkerUpgrade(request)
	if err != nil {
		return err
	}
	return errors.WithStack(json.NewEncoder(output).Encode(result))
}
