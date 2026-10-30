package peersession

import (
	"context"
	"crypto/rand"
	"net"
	"slices"
	"strings"
	"sync"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/credentials"
)

const maxGroupsPerConnection = 64
const maxBootHistory = 1024
const maxMethodSize = 1024

type Owner struct {
	wire.UnimplementedPeerSessionServer
	mu          sync.Mutex
	local       Node
	boot        Nonce
	versions    raftbootstrap.Versions
	provisioned map[raftbootstrap.Digest]Provision
	groups      map[uint64]Group
	methods     map[string]Method
	public      map[string]bool
	catchup     CatchupAuthorizer
	connections map[*connection]bool
	boots       map[bootKey]map[Nonce]bool
	currentBoot map[bootKey]Nonce
	closed      bool
}

type bootKey struct {
	Node Node
}
type phase uint8

const (
	pending  phase = 1
	complete phase = 2
)

type session struct {
	id, key       raftbootstrap.Digest
	binding       Binding
	local, remote view
	phase         phase
	ctx           context.Context
	cancel        context.CancelFunc
}

type connection struct {
	owner     *Owner
	tls       credentials.TLSInfo
	remote    *Node
	client    bool
	transport net.Conn
	closed    bool
	sessions  map[uint64]*session
	begun     map[uint64]bool
}

func NewOwner(cfg Config) (*Owner, error) {
	if !validNode(cfg.Local) || !validVersions(cfg.Versions) {
		return nil, ErrFormat
	}
	p, err := provisionMap(cfg.Local, cfg.Provisioned)
	if err != nil {
		return nil, err
	}
	o := &Owner{local: cfg.Local, versions: cfg.Versions, provisioned: p, groups: make(map[uint64]Group),
		methods: make(map[string]Method), public: make(map[string]bool), catchup: cfg.Catchup,
		connections: make(map[*connection]bool), boots: make(map[bootKey]map[Nonce]bool), currentBoot: make(map[bootKey]Nonce)}
	if _, err := rand.Read(o.boot[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := o.configureMethods(cfg); err != nil {
		return nil, err
	}
	return o, nil
}

func provisionMap(local Node, records []Provision) (map[raftbootstrap.Digest]Provision, error) {
	p := make(map[raftbootstrap.Digest]Provision)
	ids := make(map[uint64]bool)
	for _, record := range records {
		if !validNode(record.Node) || record.Node.Cluster != local.Cluster || ids[record.Node.Member] {
			return nil, ErrFormat
		}
		if _, exists := p[record.Node.Key]; exists {
			return nil, ErrFormat
		}
		if len(record.Groups) == 0 {
			return nil, ErrFormat
		}
		record.Groups = slices.Clone(record.Groups)
		slices.Sort(record.Groups)
		if len(slices.Compact(slices.Clone(record.Groups))) != len(record.Groups) {
			return nil, ErrFormat
		}
		p[record.Node.Key] = record
		ids[record.Node.Member] = true
	}
	self, ok := p[local.Key]
	if !ok || self.Node != local {
		return nil, ErrFormat
	}
	return p, nil
}

func (o *Owner) configureMethods(cfg Config) error {
	for _, m := range cfg.Methods {
		if !validMethod(m.Name) || isHandshake(m.Name) || !validDecoders(m) {
			return ErrFormat
		}
		if _, exists := o.methods[m.Name]; exists {
			return ErrFormat
		}
		o.methods[m.Name] = m
	}
	return o.configurePublic(cfg.PublicMethods)
}

func (o *Owner) configurePublic(methods []string) error {
	for _, m := range methods {
		if !validMethod(m) || isHandshake(m) || o.public[m] {
			return ErrFormat
		}
		if _, exists := o.methods[m]; exists {
			return ErrFormat
		}
		o.public[m] = true
	}
	return nil
}

func validMethod(s string) bool {
	if len(s) > maxMethodSize || !strings.HasPrefix(s, "/") {
		return false
	}
	service, method, ok := strings.Cut(s[1:], "/")
	return ok && service != "" && method != "" && !strings.ContainsAny(s, " \t\r\n\x00") && !strings.Contains(method, "/")
}
func isHandshake(m string) bool {
	return m == wire.PeerSession_Begin_FullMethodName || m == wire.PeerSession_Finish_FullMethodName
}

// UpdateGroup revokes/cancels before publication, then closes affected physical
// transports so blocked native gRPC stream receives also terminate. Call BEFORE
// exposing a changed role/policy/term/configuration/stage in the runtime owner.
// Increment Generation for every such change, including a term-only change.
func (o *Owner) UpdateGroup(g Group) error {
	if err := validateGroup(g); err != nil {
		return err
	}
	g = copyGroup(g)
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrClosed
	}
	if old, ok := o.groups[g.ID]; ok {
		if err := validateGroupUpdate(old, g); err != nil {
			o.mu.Unlock()
			return err
		}
	}
	transports := o.revokeGroupLocked(g.ID)
	o.groups[g.ID] = g
	o.mu.Unlock()
	closeTransports(transports)
	return nil
}

func validateGroupUpdate(old, next Group) error {
	if next.Generation <= old.Generation || next.Floor.Generation < old.Floor.Generation {
		return ErrDenied
	}
	if old.Floor.ActivationIndex != 0 && next.Floor.ActivationIndex != old.Floor.ActivationIndex {
		return ErrDenied
	}
	return nil
}

// ReplaceProvisioning revokes all old peer sessions before replacing PKI mapping.
// A certificate identifies a provisioned key, never an executable/version.
func (o *Owner) ReplaceProvisioning(records []Provision) error {
	p, err := provisionMap(o.local, records)
	if err != nil {
		return err
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrClosed
	}
	var transports []net.Conn
	for c := range o.connections {
		if c.remote != nil {
			transports = append(transports, o.detachLocked(c))
		}
	}
	o.provisioned = p
	o.mu.Unlock()
	closeTransports(transports)
	return nil
}

func (o *Owner) revokeGroupLocked(group uint64) []net.Conn {
	var transports []net.Conn
	for c := range o.connections {
		if c.sessions[group] != nil {
			transports = append(transports, o.detachLocked(c))
		}
	}
	return transports
}

// RevokeGroup must precede a catch-up grant/term/epoch owner's publication of
// revoked authority, even if no replacement Group is available yet.
func (o *Owner) RevokeGroup(group uint64) {
	o.mu.Lock()
	transports := o.revokeGroupLocked(group)
	o.mu.Unlock()
	closeTransports(transports)
}

func (o *Owner) detachLocked(c *connection) net.Conn {
	if c.closed {
		return nil
	}
	c.closed = true
	delete(o.connections, c)
	for _, s := range c.sessions {
		s.cancel()
		clear(s.key[:])
	}
	clear(c.sessions)
	transport := c.transport
	c.transport = nil
	c.tls = credentials.TLSInfo{}
	return transport
}

func closeTransports(conns []net.Conn) {
	for _, c := range conns {
		if c != nil {
			_ = c.Close()
		}
	}
}

func (o *Owner) closeConnection(c *connection) {
	o.mu.Lock()
	transport := o.detachLocked(c)
	o.mu.Unlock()
	closeTransports([]net.Conn{transport})
}

// Close is idempotent and destroys all sessions and their exporter-derived keys.
func (o *Owner) Close() {
	o.mu.Lock()
	o.closed = true
	var transports []net.Conn
	for c := range o.connections {
		transports = append(transports, o.detachLocked(c))
	}
	o.mu.Unlock()
	closeTransports(transports)
}

type Counts struct{ Connections, Pending, Active int }

func (o *Owner) Counts() Counts {
	o.mu.Lock()
	defer o.mu.Unlock()
	r := Counts{Connections: len(o.connections)}
	for c := range o.connections {
		for _, s := range c.sessions {
			if s.phase == complete {
				r.Active++
			} else {
				r.Pending++
			}
		}
	}
	return r
}

func (o *Owner) allowedLocked(n Node, group uint64) bool {
	p, ok := o.provisioned[n.Key]
	return ok && p.Node == n && slices.Contains(p.Groups, group)
}

func (o *Owner) groupViewLocked(group uint64) (view, error) {
	g, ok := o.groups[group]
	if !ok || !o.allowedLocked(o.local, group) {
		return view{}, ErrDenied
	}
	digest, err := raftbootstrap.ConfigurationDigest(g.Configuration)
	if err != nil {
		return view{}, errors.WithStack(err)
	}
	return view{Installed: o.versions, Minimum: g.Minimum, Floor: g.Floor, Policy: g.Policy, Configuration: digest,
		Generation: g.Generation, State: g.State, MarkerReady: g.MarkerReady}, nil
}

func (o *Owner) newSessionLocked(c *connection, group uint64, id, key raftbootstrap.Digest, local, remote view, remoteBoot Nonce) *session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &session{id: id, key: key, local: local, remote: remote, phase: pending, ctx: ctx, cancel: cancel,
		binding: Binding{Session: id, Group: group, Generation: local.Generation, Local: o.local, Remote: *c.remote,
			LocalBoot: o.boot, RemoteBoot: remoteBoot, Configuration: local.Configuration, Policy: local.Policy}}
	c.sessions[group] = s
	return s
}

func (o *Owner) acceptBootLocked(c *connection, boot Nonce) ([]net.Conn, error) {
	for _, s := range c.sessions {
		if s.binding.RemoteBoot != boot {
			return nil, ErrDenied
		}
	}
	k := bootKey{Node: *c.remote}
	old, exists := o.currentBoot[k]
	if exists && old == boot {
		return nil, nil
	}
	seen := o.boots[k]
	if seen == nil {
		seen = make(map[Nonce]bool)
		o.boots[k] = seen
	}
	if seen[boot] || len(seen) >= maxBootHistory {
		return nil, ErrDenied
	}
	seen[boot] = true
	o.currentBoot[k] = boot
	return o.retireOldBootLocked(c), nil
}

func (o *Owner) retireOldBootLocked(c *connection) []net.Conn {
	var transports []net.Conn
	for other := range o.connections {
		if other != c && other.remote != nil && *other.remote == *c.remote && len(other.sessions) != 0 {
			transports = append(transports, o.detachLocked(other))
		}
	}
	return transports
}
