package adapter

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTransferPreflightOwnershipReadbackOnceBeforeControl(t *testing.T) {
	for _, missingLearner := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "learner-missing"}[missingLearner], func(t *testing.T) {
			f := newTransferAdmissionFixture(t)
			ownership, controls := 0, 0
			for index := range f.snapshot.Members {
				member := &f.snapshot.Members[index]
				client, ok := member.Client.(*transferAdmissionClient)
				require.True(t, ok)
				original := client.probe
				learner := member.ID == "learner"
				client.probe = func(req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
					resp, err := original(req)
					if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED {
						ownership++
						if learner && missingLearner {
							resp.Ready = false
						}
					} else if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED {
						require.Equal(t, len(f.snapshot.Members), ownership, "all ownership ACKs must precede control readback")
						controls++
					}
					return resp, err
				}
			}
			f.source.probe = func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				return &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41, Ownership: proto.CloneOf(f.proof)}, nil
			}
			witness, err := f.server.ensureSplitJobTransferControl(context.Background(), f.job, 2, f.source, f.control, nil, splitTransferAdmissionOptions{readExistingControl: true})
			if missingLearner {
				require.Error(t, err)
				require.Nil(t, witness)
				require.Zero(t, controls)
			} else {
				require.NoError(t, err)
				require.NotNil(t, witness)
				require.Equal(t, len(f.snapshot.Members), ownership)
				require.Equal(t, len(f.snapshot.Members), controls)
			}
			require.Empty(t, f.applied)
		})
	}
}
