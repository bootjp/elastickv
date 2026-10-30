package raftbootstrap

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

const (
	IntentFile                 = "bootstrap-intent"
	MarkerFile                 = "raft-engine"
	MarkerV1                   = "etcd-bootstrap-v1\n"
	MaxFileSize                = 16 << 20
	intentMagic                = "EKVBI001"
	formatVersion       uint32 = 1
	stateRequired       uint32 = 1
	checksumSize               = sha256.Size
	frameOverhead              = len(intentMagic) + 4 + checksumSize
	memberIDSize               = 8
	intentPrefixSize           = 4 + 4 + 16 + 8 + 4 + 12 + 16
	configurationLimit         = MaxFileSize - frameOverhead - intentPrefixSize
	configurationHeader        = 8 + 1 + 4*4 + 4
	memberHeader               = 8 + 4 + checksumSize
)

var (
	ErrInvalid      = errors.New("invalid bootstrap format")
	ErrUnsupported  = errors.New("unsupported bootstrap format")
	ErrInconsistent = errors.New("inconsistent bootstrap evidence")
)

type UUID [16]byte
type Digest [sha256.Size]byte

// ParseUUID accepts only the canonical lower-case hyphenated UUID spelling.
func ParseUUID(s string) (UUID, error) {
	var id UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' || strings.ToLower(s) != s {
		return id, ErrInvalid
	}
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != len(id) {
		return id, ErrInvalid
	}
	copy(id[:], b)
	if id == (UUID{}) {
		return id, ErrInvalid
	}
	return id, nil
}

type Identity struct {
	Cluster UUID
	Group   uint64 // Group zero is valid.
}

type Versions struct{ Entry, Snapshot, Session uint32 }
type Floor struct{ ActivationIndex, Generation uint64 }

type BaselineKind uint32

const (
	Seed     BaselineKind = 1
	Accepted BaselineKind = 2
)

type ConfState struct {
	Voters, VotersOutgoing, Learners, LearnersNext []uint64
	AutoLeave                                      bool
}

type Member struct {
	ID              uint64
	Address         string
	PublicKeyDigest Digest
}

// Configuration is the provisioned seed or explicitly accepted baseline, not
// an assertion of historical replica equality. Boot nonces never belong here.
type Configuration struct {
	Index   uint64
	State   ConfState
	Members []Member
}

type Intent struct {
	Identity     Identity
	BaselineKind BaselineKind
	Baseline     Configuration
	Minimum      Versions
	Floor        Floor
}

func validateVersions(v Versions) error {
	if v.Entry == 0 || v.Snapshot == 0 || v.Session == 0 {
		return ErrInvalid
	}
	if v != (Versions{Entry: 1, Snapshot: 1, Session: 1}) {
		return ErrUnsupported
	}
	return nil
}

func validFloor(f Floor) bool { return (f.ActivationIndex == 0) == (f.Generation == 0) }

func canonicalIntent(in Intent) (Intent, error) {
	if in.Identity.Cluster == (UUID{}) || !validFloor(in.Floor) {
		return Intent{}, ErrInvalid
	}
	if err := validateVersions(in.Minimum); err != nil {
		return Intent{}, err
	}
	if err := validateBaselineFloor(in); err != nil {
		return Intent{}, err
	}
	if err := validateBaselineKind(in); err != nil {
		return Intent{}, err
	}
	c, err := canonicalConfiguration(in.Baseline)
	if err != nil {
		return Intent{}, err
	}
	in.Baseline = c
	return in, nil
}

func validateBaselineKind(in Intent) error {
	switch in.BaselineKind {
	case Seed:
		if in.Baseline.Index != 0 {
			return ErrInvalid
		}
	case Accepted:
		if in.Baseline.Index == 0 {
			return ErrInvalid
		}
	default:
		return ErrUnsupported
	}
	return nil
}

func validateBaselineFloor(in Intent) error {
	if in.Floor.ActivationIndex != 0 && in.Floor.ActivationIndex <= in.Baseline.Index {
		return ErrInvalid
	}
	return nil
}

func canonicalConfiguration(c Configuration) (Configuration, error) {
	if _, err := configurationEncodedSize(c); err != nil {
		return Configuration{}, err
	}
	sets := []*[]uint64{&c.State.Voters, &c.State.VotersOutgoing, &c.State.Learners, &c.State.LearnersNext}
	all := make(map[uint64]bool)
	for _, set := range sets {
		*set = slices.Clone(*set)
		slices.Sort(*set)
		if err := validateIDs(*set); err != nil {
			return Configuration{}, err
		}
		for _, id := range *set {
			all[id] = true
		}
	}
	if err := validateRoles(c.State); err != nil {
		return Configuration{}, err
	}
	c.Members = slices.Clone(c.Members)
	slices.SortFunc(c.Members, func(a, b Member) int { return compareID(a.ID, b.ID) })
	return c, validateMembers(c.Members, all)
}

// Check the aggregate wire size before cloning, sorting or serializing. A
// caller can share one large string between many members, so input ownership
// does not bound the output. The subtraction checks also avoid integer wrap.
func configurationEncodedSize(c Configuration) (int, error) {
	size := configurationHeader
	for _, ids := range [][]uint64{c.State.Voters, c.State.VotersOutgoing, c.State.Learners, c.State.LearnersNext} {
		if len(ids) > (configurationLimit-size)/memberIDSize {
			return 0, ErrInvalid
		}
		size += len(ids) * memberIDSize
	}
	for _, member := range c.Members {
		remaining := configurationLimit - size
		if remaining < memberHeader || len(member.Address) > remaining-memberHeader {
			return 0, ErrInvalid
		}
		size += memberHeader + len(member.Address)
	}
	return size, nil
}

func validateIDs(ids []uint64) error {
	for i, id := range ids {
		if id == 0 || (i > 0 && ids[i-1] == id) {
			return ErrInvalid
		}
	}
	return nil
}

func validateMembers(members []Member, all map[uint64]bool) error {
	if len(members) != len(all) {
		return ErrInvalid
	}
	for i, m := range members {
		if !all[m.ID] || (i > 0 && members[i-1].ID == m.ID) {
			return ErrInvalid
		}
		if m.PublicKeyDigest == (Digest{}) || !validAddress(m.Address) {
			return ErrInvalid
		}
	}
	return nil
}

func compareID(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func validAddress(address string) bool {
	if !utf8.ValidString(address) {
		return false
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || strings.ContainsAny(address, " \t\r\n\x00") {
		return false
	}
	n, err := strconv.ParseUint(port, 10, 16)
	return err == nil && n != 0
}

func validateRoles(s ConfState) error {
	if len(s.Voters) == 0 || (len(s.VotersOutgoing) == 0 && (s.AutoLeave || len(s.LearnersNext) != 0)) {
		return ErrInvalid
	}
	if err := validateLearners(s); err != nil {
		return err
	}
	for _, id := range s.LearnersNext {
		if containsID(s.Voters, id) || !containsID(s.VotersOutgoing, id) {
			return ErrInvalid
		}
	}
	return nil
}

func validateLearners(s ConfState) error {
	for _, id := range s.Learners {
		if containsID(s.Voters, id) || containsID(s.VotersOutgoing, id) || containsID(s.LearnersNext, id) {
			return ErrInvalid
		}
	}
	return nil
}

func containsID(ids []uint64, id uint64) bool { _, ok := slices.BinarySearch(ids, id); return ok }

// MarshalIntent clones and canonicalizes the input. The SHA-256 covers both
// the frame header and the payload; it detects corruption, not malicious edits.
func MarshalIntent(in Intent) ([]byte, error) {
	in, err := canonicalIntent(in)
	if err != nil {
		return nil, err
	}
	w := &encoder{limit: MaxFileSize - frameOverhead}
	w.fixed(formatVersion)
	w.fixed(stateRequired)
	w.fixed(in.Identity.Cluster)
	w.fixed(in.Identity.Group)
	w.fixed(in.BaselineKind)
	w.fixed(in.Minimum)
	w.fixed(in.Floor)
	encodeConfiguration(w, in.Baseline)
	if w.err != nil {
		return nil, w.err
	}
	frame := &encoder{limit: MaxFileSize}
	frame.appendString(intentMagic)
	frame.length(w.Len())
	if frame.err != nil || w.Len() > MaxFileSize-frameOverhead {
		return nil, ErrInvalid
	}
	frame.appendBytes(w.Bytes())
	sum := sha256.Sum256(frame.Bytes())
	frame.appendBytes(sum[:])
	if frame.err != nil {
		return nil, frame.err
	}
	return frame.Bytes(), nil
}

type encoder struct {
	buffer bytes.Buffer
	limit  int
	err    error
}

func (w *encoder) Len() int      { return w.buffer.Len() }
func (w *encoder) Bytes() []byte { return w.buffer.Bytes() }

func (w *encoder) reserve(n int) bool {
	if w.err != nil {
		return false
	}
	limit := w.limit
	if limit == 0 {
		limit = configurationLimit
	}
	if n < 0 || n > limit-w.Len() {
		w.err = ErrInvalid
		return false
	}
	return true
}

func (w *encoder) appendBytes(b []byte) {
	if w.reserve(len(b)) {
		_, w.err = w.buffer.Write(b)
	}
}

func (w *encoder) appendString(s string) {
	if w.reserve(len(s)) {
		_, w.err = w.buffer.WriteString(s)
	}
}

func (w *encoder) fixed(v any) {
	if w.reserve(binary.Size(v)) {
		w.err = errors.WithStack(binary.Write(&w.buffer, binary.BigEndian, v))
	}
}

func (w *encoder) length(n int) {
	if n < 0 || n > MaxFileSize {
		w.err = ErrInvalid
		return
	}
	w.fixed(uint32(n))
}

func encodeConfiguration(w *encoder, c Configuration) {
	w.fixed(c.Index)
	w.fixed(c.State.AutoLeave)
	for _, ids := range [][]uint64{c.State.Voters, c.State.VotersOutgoing, c.State.Learners, c.State.LearnersNext} {
		w.length(len(ids))
		for _, id := range ids {
			w.fixed(id)
		}
	}
	w.length(len(c.Members))
	for _, m := range c.Members {
		w.fixed(m.ID)
		w.length(len(m.Address))
		w.appendString(m.Address)
		w.fixed(m.PublicKeyDigest)
	}
}

// ConfigurationDigest hashes the canonical baseline without identity/nonces.
func ConfigurationDigest(c Configuration) (Digest, error) {
	c, err := canonicalConfiguration(c)
	if err != nil {
		return Digest{}, err
	}
	w := &encoder{}
	encodeConfiguration(w, c)
	if w.err != nil {
		return Digest{}, w.err
	}
	return sha256.Sum256(w.Bytes()), nil
}

func ParseIntent(data []byte) (Intent, error) {
	var in Intent
	payload, err := parseFrame(data)
	if err != nil {
		return in, err
	}
	r := &decoder{Reader: bytes.NewReader(payload)}
	var version, state uint32
	r.fixed(&version)
	r.fixed(&state)
	if version != formatVersion || state != stateRequired {
		return in, ErrUnsupported
	}
	r.fixed(&in.Identity.Cluster)
	r.fixed(&in.Identity.Group)
	r.fixed(&in.BaselineKind)
	r.fixed(&in.Minimum)
	r.fixed(&in.Floor)
	in.Baseline = decodeConfiguration(r)
	if r.err != nil || r.Len() != 0 {
		return Intent{}, ErrInvalid
	}
	canonical, err := MarshalIntent(in)
	if err != nil {
		return Intent{}, err
	}
	if !bytes.Equal(canonical, data) {
		return Intent{}, ErrInvalid
	}
	return in, nil
}

func parseFrame(data []byte) ([]byte, error) {
	if len(data) > MaxFileSize || len(data) < frameOverhead {
		return nil, ErrInvalid
	}
	if string(data[:len(intentMagic)]) != intentMagic {
		return nil, ErrUnsupported
	}
	size := binary.BigEndian.Uint32(data[len(intentMagic):])
	if int64(size) != int64(len(data))-int64(frameOverhead) {
		return nil, ErrInvalid
	}
	end := len(data) - checksumSize
	sum := sha256.Sum256(data[:end])
	if !bytes.Equal(sum[:], data[end:]) {
		return nil, ErrInvalid
	}
	return data[len(intentMagic)+4 : end], nil
}

type decoder struct {
	*bytes.Reader
	err error
}

func (r *decoder) fixed(v any) {
	if r.err == nil {
		r.err = errors.WithStack(binary.Read(r.Reader, binary.BigEndian, v))
	}
}

func (r *decoder) count(minSize uint32) uint32 {
	var n uint32
	r.fixed(&n)
	if int64(n) > int64(r.Len())/int64(minSize) {
		r.err = ErrInvalid
		return 0
	}
	return n
}

func decodeConfiguration(r *decoder) Configuration {
	var c Configuration
	r.fixed(&c.Index)
	r.fixed(&c.State.AutoLeave)
	for _, ids := range []*[]uint64{&c.State.Voters, &c.State.VotersOutgoing, &c.State.Learners, &c.State.LearnersNext} {
		n := r.count(memberIDSize)
		*ids = make([]uint64, n)
		for i := range *ids {
			r.fixed(&(*ids)[i])
		}
	}
	n := r.count(8 + 4 + checksumSize)
	c.Members = make([]Member, n)
	for i := range c.Members {
		m := &c.Members[i]
		r.fixed(&m.ID)
		size := r.count(1)
		address := make([]byte, size)
		if r.err == nil {
			_, r.err = io.ReadFull(r.Reader, address)
		}
		m.Address = string(address)
		r.fixed(&m.PublicKeyDigest)
	}
	return c
}
