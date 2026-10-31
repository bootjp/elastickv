package raftbootstrap

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func put(t *testing.T, dir, name string, b []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), b, filePerm))
}

func diskBytes(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	return b
}

func active(t *testing.T) (Intent, Evidence) {
	t.Helper()
	in, e := fixture(t)
	in.Floor = Floor{ActivationIndex: 11, Generation: 2}
	e.Policy = &Policy{Identity: in.Identity, BaselineDigest: *e.BaselineDigest, Minimum: in.Minimum, Floor: in.Floor}
	return in, e
}

func TestPublishAndInspect(t *testing.T) {
	in, e := fixture(t)
	dir := t.TempDir()
	s := NewStore(dir)
	r, err := s.Inspect(e)
	require.NoError(t, err)
	require.Equal(t, Absent, r.Class)
	put(t, dir, MarkerFile, []byte("etcd\n"))
	r, err = s.Inspect(e)
	require.NoError(t, err)
	require.Equal(t, Legacy, r.Class)
	require.NoError(t, s.Publish(in, e))
	require.Equal(t, []byte(MarkerV1), diskBytes(t, dir, MarkerFile))
	for _, name := range []string{IntentFile, MarkerFile} {
		info, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(filePerm), info.Mode().Perm())
	}
	r, err = s.Inspect(e)
	require.NoError(t, err)
	require.Equal(t, Required, r.Class)
	r, err = s.Inspect(Evidence{})
	require.NoError(t, err)
	require.Equal(t, Recovery, r.Class)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.NoError(t, s.Publish(in, e), "idempotent retry still syncs both files")
}

func TestClassifyPartialPairs(t *testing.T) {
	in, e := fixture(t)
	b, err := MarshalIntent(in)
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		marker  []byte
		intent  []byte
		want    Classification
		wantErr error
	}{
		{"absent", nil, nil, Absent, nil},
		{"legacy", []byte("etcd\n"), nil, Legacy, nil},
		{"intent-only", nil, b, Recovery, nil},
		{"intent-and-legacy", []byte("etcd\n"), b, Recovery, nil},
		{"marker-only", []byte(MarkerV1), nil, Recovery, nil},
		{"complete", []byte(MarkerV1), b, Required, nil},
		{"empty-marker", []byte{}, b, Corrupt, ErrInvalid},
		{"unknown-marker", []byte("future\n"), b, Unsupported, ErrUnsupported},
		{"truncated-marker", []byte("etcd-bootstrap-v"), b, Unsupported, ErrUnsupported},
		{"empty-intent", []byte(MarkerV1), []byte{}, Corrupt, ErrInvalid},
		{"corrupt-intent", []byte(MarkerV1), b[:len(b)-1], Corrupt, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.marker != nil {
				put(t, dir, MarkerFile, tc.marker)
			}
			if tc.intent != nil {
				put(t, dir, IntentFile, tc.intent)
			}
			s := NewStore(dir)
			r, err := s.Inspect(e)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.want, r.Class)
			if tc.wantErr != nil {
				require.Error(t, s.Publish(in, e))
				require.Equal(t, tc.marker, diskBytes(t, dir, MarkerFile))
			}
		})
	}
}

func TestEvidenceCrossChecks(t *testing.T) {
	in, e := active(t)
	for _, tc := range []struct {
		name    string
		change  func(*Evidence)
		want    Classification
		wantErr error
	}{
		{"missing-wal", func(e *Evidence) { e.RaftState = Lost }, LostState, nil},
		{"unverified-wal", func(e *Evidence) { e.RaftState = Unverified }, Recovery, nil},
		{"missing-policy", func(e *Evidence) { e.Policy = nil }, Recovery, nil},
		{"missing-identity", func(e *Evidence) { e.Identity = nil }, Recovery, nil},
		{"missing-baseline", func(e *Evidence) { e.BaselineDigest = nil }, Recovery, nil},
		{"wrong-group", func(e *Evidence) { id := *e.Identity; id.Group++; e.Identity = &id }, Inconsistent, ErrInconsistent},
		{"wrong-cluster", func(e *Evidence) { id := *e.Identity; id.Cluster[0]++; e.Identity = &id }, Inconsistent, ErrInconsistent},
		{"wrong-baseline", func(e *Evidence) { d := *e.BaselineDigest; d[0]++; e.BaselineDigest = &d }, Inconsistent, ErrInconsistent},
		{"policy-identity", func(e *Evidence) { e.Policy.Identity.Group++ }, Inconsistent, ErrInconsistent},
		{"policy-baseline", func(e *Evidence) { e.Policy.BaselineDigest[0]++ }, Inconsistent, ErrInconsistent},
		{"policy-versions", func(e *Evidence) { e.Policy.Minimum.Session++ }, Unsupported, ErrUnsupported},
		{"rollback-generation", func(e *Evidence) { e.Policy.Floor.Generation-- }, Inconsistent, ErrInconsistent},
		{"different-activation", func(e *Evidence) { e.Policy.Floor.ActivationIndex++ }, Inconsistent, ErrInconsistent},
		{"unactivated-policy", func(e *Evidence) { e.Policy.Floor = Floor{} }, Inconsistent, ErrInconsistent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := NewStore(dir)
			require.NoError(t, s.Publish(in, e))
			changed := e
			p := *e.Policy
			changed.Policy = &p
			tc.change(&changed)
			r, err := s.Inspect(changed)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.want, r.Class)
			require.Error(t, s.Publish(in, changed))
		})
	}
}

func TestPolicyFloorAndMissingMarkerRecovery(t *testing.T) {
	in, e := fixture(t)
	dir := t.TempDir()
	s := NewStore(dir)
	require.NoError(t, s.Publish(in, e))
	updated, recovered := active(t)
	r, err := s.Inspect(recovered)
	require.NoError(t, err)
	require.Equal(t, Recovery, r.Class)
	require.NoError(t, s.Publish(updated, recovered))
	r, err = s.Inspect(recovered)
	require.NoError(t, err)
	require.Equal(t, Required, r.Class)
	require.NoError(t, os.Remove(filepath.Join(dir, MarkerFile)))
	r, err = s.Inspect(recovered)
	require.NoError(t, err)
	require.Equal(t, Recovery, r.Class)
	require.NoError(t, s.Publish(updated, recovered))
	require.Equal(t, []byte(MarkerV1), diskBytes(t, dir, MarkerFile))
	require.ErrorIs(t, s.Publish(in, e), ErrInconsistent, "cannot lower durable floor")
	// Both local files missing with retained P is recovery-only, never fresh.
	require.NoError(t, os.Remove(filepath.Join(dir, IntentFile)))
	require.NoError(t, os.Remove(filepath.Join(dir, MarkerFile)))
	r, err = s.Inspect(recovered)
	require.NoError(t, err)
	require.Equal(t, Recovery, r.Class)
	require.NoError(t, s.Publish(updated, recovered))
}

func TestMarkerOnlyNeedsPolicy(t *testing.T) {
	in, e := fixture(t)
	dir := t.TempDir()
	s := NewStore(dir)
	put(t, dir, MarkerFile, []byte(MarkerV1))
	require.ErrorIs(t, s.Publish(in, e), ErrInconsistent)
	in, e = active(t)
	require.NoError(t, s.Publish(in, e))
}

func TestRequiredRepairNeverRestoresLostVotingHistory(t *testing.T) {
	for _, state := range []RaftState{Lost, Unverified} {
		in, e := active(t)
		e.RaftState = state
		dir := t.TempDir()
		s := NewStore(dir)
		// Retained P with both local files missing is not permission to seed.
		require.ErrorIs(t, s.Publish(in, e), ErrInconsistent)
		require.NoError(t, s.RepairRequired(in, e))
		require.Equal(t, []byte(MarkerV1), diskBytes(t, dir, MarkerFile))
		r, err := s.Inspect(e)
		require.NoError(t, err)
		if state == Lost {
			require.Equal(t, LostState, r.Class)
		} else {
			require.Equal(t, Recovery, r.Class)
		}
		e.Policy = nil
		require.ErrorIs(t, s.RepairRequired(in, e), ErrInconsistent)
	}
}

func TestExplicitSeedAndLostState(t *testing.T) {
	in, e := fixture(t)
	in.BaselineKind = Seed
	in.Baseline.Index = 0
	digest, err := ConfigurationDigest(in.Baseline)
	require.NoError(t, err)
	e.BaselineDigest = &digest
	e.RaftState = ProvisionedSeed
	dir := t.TempDir()
	s := NewStore(dir)
	require.NoError(t, s.Publish(in, e))
	r, err := s.Inspect(e)
	require.NoError(t, err)
	require.Equal(t, Required, r.Class)
	e.RaftState = Lost
	r, err = s.Inspect(e)
	require.NoError(t, err)
	require.Equal(t, LostState, r.Class)
	require.ErrorIs(t, s.Publish(in, e), ErrInconsistent)
	s = NewStore(t.TempDir())
	e.RaftState = Intact
	require.ErrorIs(t, s.Publish(in, e), ErrInconsistent, "intact established identity is not cold seed authorization")
	e.RaftState = Unverified
	require.ErrorIs(t, s.Publish(in, e), ErrInconsistent)
	e.RaftState = ProvisionedSeed
	put(t, s.dir, MarkerFile, []byte("etcd\n"))
	require.ErrorIs(t, s.Publish(in, e), ErrInconsistent, "legacy cannot become fresh seed")
}

func TestRejectUnsafeFilesWithoutOverwrite(t *testing.T) {
	for _, name := range []string{IntentFile, MarkerFile} {
		for _, kind := range []string{"mode", "symlink", "directory", "oversize"} {
			t.Run(name+"/"+kind, func(t *testing.T) {
				in, e := fixture(t)
				dir := t.TempDir()
				path := filepath.Join(dir, name)
				switch kind {
				case "mode":
					put(t, dir, name, []byte("etcd\n"))
					require.NoError(t, os.Chmod(path, 0o644))
				case "symlink":
					put(t, dir, "target", []byte("secret"))
					require.NoError(t, os.Symlink(filepath.Join(dir, "target"), path))
				case "directory":
					require.NoError(t, os.Mkdir(path, 0o700))
				case "oversize":
					put(t, dir, name, bytes.Repeat([]byte{0}, MaxFileSize+1))
				}
				s := NewStore(dir)
				r, err := s.Inspect(e)
				require.Error(t, err)
				require.Equal(t, Corrupt, r.Class)
				require.Error(t, s.Publish(in, e))
				if kind == "symlink" {
					require.Equal(t, []byte("secret"), diskBytes(t, dir, "target"))
				}
			})
		}
	}
}

func TestConcurrentSingleOwner(t *testing.T) {
	in, e := fixture(t)
	s := NewStore(t.TempDir())
	require.NoError(t, s.Publish(in, e))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10 {
				require.NoError(t, s.Publish(in, e))
				r, err := s.Inspect(e)
				require.NoError(t, err)
				require.Equal(t, Required, r.Class)
			}
		})
	}
	wg.Wait()
}
