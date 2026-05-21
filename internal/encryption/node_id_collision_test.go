package encryption_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
)

// TestCheckNodeIDCollision_Empty pins the skip-on-empty case:
// with no full_node_ids the primitive cannot detect a collision
// and MUST return nil. The caller (startup-guard wiring) is
// responsible for the encryption-disabled and
// pre-bootstrap-empty-membership skip conditions.
func TestCheckNodeIDCollision_Empty(t *testing.T) {
	t.Parallel()
	if err := encryption.CheckNodeIDCollision(nil); err != nil {
		t.Errorf("nil slice: want nil, got %v", err)
	}
	if err := encryption.CheckNodeIDCollision([]uint64{}); err != nil {
		t.Errorf("empty slice: want nil, got %v", err)
	}
}

// TestCheckNodeIDCollision_SingleNode pins the single-member
// case: one full_node_id alone cannot collide with itself, so
// the primitive returns nil.
func TestCheckNodeIDCollision_SingleNode(t *testing.T) {
	t.Parallel()
	if err := encryption.CheckNodeIDCollision([]uint64{0xAAAA}); err != nil {
		t.Errorf("single node: want nil, got %v", err)
	}
}

// TestCheckNodeIDCollision_NoCollision pins the healthy
// multi-node case: every distinct full_node_id maps to a
// distinct 16-bit node_id.
func TestCheckNodeIDCollision_NoCollision(t *testing.T) {
	t.Parallel()
	// Three values whose low 16 bits are clearly distinct
	// (0x0001 / 0x0002 / 0x0003).
	fnids := []uint64{
		0xDEADBEEF_00000001,
		0xDEADBEEF_00000002,
		0xDEADBEEF_00000003,
	}
	if err := encryption.CheckNodeIDCollision(fnids); err != nil {
		t.Errorf("no collision: want nil, got %v", err)
	}
}

// TestCheckNodeIDCollision_Collision pins the fire path: two
// DISTINCT full_node_id values whose low 16 bits match. The
// classic hit is two values that differ only above bit 16.
func TestCheckNodeIDCollision_Collision(t *testing.T) {
	t.Parallel()
	// 0xDEADBEEF_0000_AAAA and 0xCAFEBABE_0000_AAAA both narrow
	// to node_id 0xAAAA.
	fnids := []uint64{
		0xDEADBEEF_0000AAAA,
		0xCAFEBABE_0000AAAA,
	}
	err := encryption.CheckNodeIDCollision(fnids)
	if !errors.Is(err, encryption.ErrNodeIDCollision) {
		t.Fatalf("collision: want ErrNodeIDCollision, got %v", err)
	}
}

// TestCheckNodeIDCollision_DuplicateNotCollision verifies that
// the SAME full_node_id appearing twice in the slice (which a
// route-catalog watcher should already dedupe but the primitive
// defends against under any input ordering) is NOT reported as
// a collision. Only DISTINCT full_node_ids mapping to the same
// node_id count.
func TestCheckNodeIDCollision_DuplicateNotCollision(t *testing.T) {
	t.Parallel()
	fnids := []uint64{0xAAAA, 0xAAAA, 0xBBBB}
	if err := encryption.CheckNodeIDCollision(fnids); err != nil {
		t.Errorf("duplicates of same full_node_id: want nil, got %v", err)
	}
}

// TestCheckNodeIDCollision_OrderIndependent pins the symmetry
// property: detection does not depend on which colliding value
// appeared first in the slice. The route catalog may emit
// members in any order; the guard must catch the collision
// either way.
func TestCheckNodeIDCollision_OrderIndependent(t *testing.T) {
	t.Parallel()
	a := []uint64{0xDEADBEEF_0000AAAA, 0xCAFEBABE_0000AAAA}
	b := []uint64{0xCAFEBABE_0000AAAA, 0xDEADBEEF_0000AAAA}
	if err := encryption.CheckNodeIDCollision(a); !errors.Is(err, encryption.ErrNodeIDCollision) {
		t.Fatalf("order A: want ErrNodeIDCollision, got %v", err)
	}
	if err := encryption.CheckNodeIDCollision(b); !errors.Is(err, encryption.ErrNodeIDCollision) {
		t.Fatalf("order B: want ErrNodeIDCollision, got %v", err)
	}
}

// TestCheckNodeIDCollision_ErrorIncludesBothIDs pins the
// operator-triage shape: the wrapped error must name BOTH
// colliding full_node_id values and the shared node_id so the
// operator can re-roll the right one without guessing.
func TestCheckNodeIDCollision_ErrorIncludesBothIDs(t *testing.T) {
	t.Parallel()
	fnids := []uint64{
		0xDEADBEEF_0000AAAA,
		0xCAFEBABE_0000AAAA,
	}
	err := encryption.CheckNodeIDCollision(fnids)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"0xdeadbeef0000aaaa",
		"0xcafebabe0000aaaa",
		"0xaaaa",
	} {
		if !strings.Contains(msg, needle) {
			t.Errorf("error message missing %q: %s", needle, msg)
		}
	}
}
