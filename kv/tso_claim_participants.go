package kv

import (
	"sort"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

func canonicalClaimParticipant(part *pb.TimestampClaimParticipant) (*pb.TimestampClaimParticipant, error) {
	if part == nil || part.GroupId == 0 || len(part.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	out := proto.CloneOf(part)
	var err error
	out.Mutations, err = canonicalClaimMutations(out.Mutations)
	if err != nil {
		return nil, err
	}
	out.ReadKeys = canonicalClaimKeys(out.ReadKeys)
	out.WriteFenceBypassKeys = canonicalClaimKeys(out.WriteFenceBypassKeys)
	return out, nil
}

func canonicalClaimParticipants(intent *pb.TimestampClaimIntent) error {
	if len(intent.Participants) == 0 || len(intent.Participants) > maxTSOClaimGrants {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	seen := make(map[uint64]struct{}, len(intent.Participants))
	for index, participant := range intent.Participants {
		part, err := canonicalClaimParticipant(participant)
		if err != nil {
			return err
		}
		if _, duplicate := seen[part.GroupId]; duplicate {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
		seen[part.GroupId] = struct{}{}
		intent.Participants[index] = part
	}
	sort.Slice(intent.Participants, func(i, j int) bool { return intent.Participants[i].GroupId < intent.Participants[j].GroupId })
	return nil
}
