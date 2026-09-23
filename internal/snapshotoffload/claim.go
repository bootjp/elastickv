package snapshotoffload

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	objectClaimBytes             = 32
	claimAcquireTimeout          = 30 * time.Second
	claimReleaseTimeout          = 30 * time.Second
	objectClaimInitialRetryDelay = 25 * time.Millisecond
	objectClaimMaxRetryDelay     = time.Second
	objectClaimBackoffMultiplier = 2
)

type claimBackingStore interface {
	ObjectStore
	DeleteObjectIfUnmodified(ctx context.Context, key string, cond DeletePrecondition) error
}

type storeObjectClaim struct {
	store claimBackingStore
	key   string
	cond  DeletePrecondition
}

// AcquireObjectClaim acquires a storage-visible claim for a local object key.
func (s *LocalStore) AcquireObjectClaim(ctx context.Context, key string) (ObjectClaim, error) {
	if _, err := s.pathForKey(key); err != nil {
		return nil, err
	}
	return acquireStoreObjectClaim(ctx, s, key)
}

// AcquireObjectClaim acquires a storage-visible claim for an S3 object key.
func (s *S3Store) AcquireObjectClaim(ctx context.Context, key string) (ObjectClaim, error) {
	if _, err := validateStoreObjectKey(key); err != nil {
		return nil, err
	}
	return acquireStoreObjectClaim(ctx, s, key)
}

func acquireStoreObjectClaim(ctx context.Context, store claimBackingStore, targetKey string) (ObjectClaim, error) {
	claimKey := objectClaimKey(targetKey)
	token := make([]byte, objectClaimBytes)
	if _, err := rand.Read(token); err != nil {
		return nil, errors.Wrap(err, "generate object claim token")
	}
	claimSHA := hexSHA256Bytes(token)
	info, err := store.PutObject(ctx, claimKey, bytes.NewReader(token), PutOptions{
		Size:        int64(len(token)),
		SHA256:      claimSHA,
		ContentType: "application/octet-stream",
	})
	if err != nil {
		if errors.Is(err, ErrIntegrity) || errors.Is(err, ErrObjectConflict) || errors.Is(err, ErrObjectNotFound) {
			return nil, errors.Wrapf(ErrObjectClaimed, "object %s", targetKey)
		}
		return nil, errors.Wrapf(err, "claim object %s", targetKey)
	}
	return &storeObjectClaim{
		store: store,
		key:   claimKey,
		cond: DeletePrecondition{
			ETag:      info.ETag,
			Size:      info.Size,
			UpdatedAt: info.UpdatedAt,
		},
	}, nil
}

func (c *storeObjectClaim) Release(ctx context.Context) error {
	if c == nil || c.store == nil {
		return nil
	}
	if err := c.store.DeleteObjectIfUnmodified(ctx, c.key, c.cond); err != nil {
		return errors.Wrap(err, "release object claim")
	}
	return nil
}

func objectClaimKey(targetKey string) string {
	normalized := normalizeObjectKey(targetKey)
	prefix := "."
	if idx := strings.LastIndex(normalized, "/v1/"); idx >= 0 {
		prefix = normalized[:idx]
	} else if strings.HasPrefix(normalized, "v1/") {
		prefix = "."
	}
	sum := sha256.Sum256([]byte(normalized))
	hexSum := hex.EncodeToString(sum[:])
	return path.Join(prefix, "v1", "claims", "sha256", hexSum[:2], hexSum+".lock")
}

func objectClaimStore(store ObjectStore) (ObjectClaimStore, error) {
	claimStore, ok := store.(ObjectClaimStore)
	if !ok {
		return nil, errors.Wrap(ErrInvalidOptions,
			"object store must support cross-process claims when snapshot publication and retention can overlap")
	}
	return claimStore, nil
}

func acquireObjectClaimWaiting(ctx context.Context, store ObjectClaimStore, key string) (ObjectClaim, error) {
	return acquireObjectClaimWithin(ctx, store, key, claimAcquireTimeout)
}

func acquireObjectClaimWithin(
	ctx context.Context, store ObjectClaimStore, key string, maxWait time.Duration,
) (ObjectClaim, error) {
	waitCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	delay := objectClaimInitialRetryDelay
	for {
		claim, err := store.AcquireObjectClaim(waitCtx, key)
		if err == nil {
			return claim, nil
		}
		if !errors.Is(err, ErrObjectClaimed) {
			return nil, errors.Wrap(err, "acquire object claim")
		}
		timer := time.NewTimer(delay)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return nil, errors.Wrapf(ErrObjectClaimed,
				"timed out waiting for object %s claim: %v", key, waitCtx.Err())
		case <-timer.C:
		}
		delay = min(delay*objectClaimBackoffMultiplier, objectClaimMaxRetryDelay)
	}
}

func releaseObjectClaim(ctx context.Context, claim ObjectClaim) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimReleaseTimeout)
	defer cancel()
	return releaseObjectClaimWithContext(releaseCtx, claim)
}

func releaseObjectClaimWithContext(ctx context.Context, claim ObjectClaim) error {
	if err := claim.Release(ctx); err != nil {
		return errors.Wrap(err, "release object claim")
	}
	return nil
}
