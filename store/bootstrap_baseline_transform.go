package store

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

var ErrBootstrapBaselineTransform = errors.New("store: baseline prepared transform mismatch")

const baselinePreparedRawPrefix = RaftControlMetadataPrefix + "J/activation-prepared/"

// VerifyBootstrapBaselinePreparedTransform is a separate, explicit offline
// migration check. It does NOT weaken baseline equality or exclude any metadata
// from DataDigest. Target must equal the complete independently preserved source
// image PLUS exactly one new trusted activation-prepared raw metadata record.
// Even an identical preexisting record is not an addition. Replacements, deletes,
// other journal/control additions and MVCC additions are all rejected.
//
// Caller owns both pinned snapshots/DBs and offline exclusivity. It must derive
// allowed from trusted preparation, never from target bytes, and independently
// authenticate the source backup/native cut and the prepared record's contents.
// This function establishes only the exact image delta, not policy correctness,
// native lineage or permission to activate. It never opens/closes the caller's
// snapshots, writes disk, parses prepared authority or treats TTL as resolution.
// Both images must still pass the unmodified base probe (encrypted unsupported).
// allowed must not be mutated during the call. It is bounded to one record with
// a nonempty value no larger than the control-record format's MaxFileSize.
func VerifyBootstrapBaselinePreparedTransform(
	ctx context.Context,
	source, target *pebble.Snapshot,
	allowed map[string][]byte,
) error {
	if ctx == nil || source == nil || target == nil {
		return errors.WithStack(ErrBootstrapBaselineTransform)
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	key, value, err := baselinePreparedAddition(allowed)
	if err != nil {
		return err
	}
	if _, err := ProbeBootstrapBaselineSnapshot(ctx, source); err != nil {
		return errors.Wrap(err, "prepared transform source")
	}
	if _, err := ProbeBootstrapBaselineSnapshot(ctx, target); err != nil {
		return errors.Wrap(err, "prepared transform target")
	}
	return verifyBaselinePreparedRows(ctx, source, target, key, value)
}

func baselinePreparedAddition(allowed map[string][]byte) ([]byte, []byte, error) {
	if len(allowed) != 1 {
		return nil, nil, errors.Wrap(ErrBootstrapBaselineTransform, "exactly one trusted addition is required")
	}
	for name, value := range allowed {
		key := []byte(name)
		identityBytes := len(raftbootstrap.UUID{}) + timestampSize
		if len(key) != len(baselinePreparedRawPrefix)+identityBytes || !bytes.HasPrefix(key, []byte(baselinePreparedRawPrefix)) {
			return nil, nil, errors.Wrap(ErrBootstrapBaselineTransform, "not an exact activation-prepared raw key")
		}
		cluster := key[len(baselinePreparedRawPrefix) : len(baselinePreparedRawPrefix)+len(raftbootstrap.UUID{})]
		if bytes.Equal(cluster, make([]byte, len(raftbootstrap.UUID{}))) {
			return nil, nil, errors.Wrap(ErrBootstrapBaselineTransform, "missing prepared cluster identity")
		}
		if len(value) == 0 || len(value) > raftbootstrap.MaxFileSize {
			return nil, nil, errors.Wrap(ErrBootstrapBaselineTransform, "empty or oversized trusted prepared value")
		}
		return key, bytes.Clone(value), nil
	}
	return nil, nil, errors.WithStack(ErrBootstrapBaselineTransform)
}

func verifyBaselinePreparedRows(ctx context.Context, source, target *pebble.Snapshot, key, value []byte) (err error) {
	sourceIter, err := source.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, sourceIter.Close()) }()
	targetIter, err := target.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, targetIter.Close()) }()
	return compareBaselinePreparedRows(ctx, sourceIter, targetIter, key, value)
}

func compareBaselinePreparedRows(ctx context.Context, source, target baselineProbeIterator, key, value []byte) (err error) {
	defer func() { err = errors.Join(err, source.Error(), target.Error(), ctx.Err()) }()
	sourceValid, targetValid := source.First(), target.First()
	added := false
	for targetValid {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		if bytes.Equal(target.Key(), key) {
			if err := checkBaselinePreparedAddition(source, sourceValid, target, key, value, added); err != nil {
				return err
			}
			added = true
		} else {
			if err := compareBaselinePreparedRow(source, sourceValid, target); err != nil {
				return err
			}
			sourceValid = source.Next()
		}
		targetValid = target.Next()
	}
	if sourceValid || !added {
		return errors.Wrap(ErrBootstrapBaselineTransform, "source row deleted or required addition missing")
	}
	return nil
}

func checkBaselinePreparedAddition(source baselineProbeIterator, sourceValid bool, target baselineProbeIterator, key, value []byte, added bool) error {
	if added || (sourceValid && bytes.Equal(source.Key(), key)) {
		return errors.Wrap(ErrBootstrapBaselineTransform, "prepared record already exists in source")
	}
	actual, err := target.ValueAndErr()
	if err != nil {
		return errors.Wrap(err, "read target prepared value")
	}
	if !bytes.Equal(actual, value) {
		return errors.Wrap(ErrBootstrapBaselineTransform, "prepared bytes differ from trusted expectation")
	}
	return nil
}

func compareBaselinePreparedRow(source baselineProbeIterator, sourceValid bool, target baselineProbeIterator) error {
	if !sourceValid || !bytes.Equal(source.Key(), target.Key()) {
		return errors.Wrap(ErrBootstrapBaselineTransform, "unapproved row addition or deletion")
	}
	sourceValue, sourceErr := source.ValueAndErr()
	targetValue, targetErr := target.ValueAndErr()
	if err := errors.Join(sourceErr, targetErr); err != nil {
		return errors.Wrap(err, "read prepared transform row")
	}
	if !bytes.Equal(sourceValue, targetValue) {
		return errors.Wrap(ErrBootstrapBaselineTransform, "source row replaced")
	}
	return nil
}
