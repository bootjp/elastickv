package peersession

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	MaxControlMessageSize       = raftbootstrap.MaxFileSize
	maxBody                     = MaxControlMessageSize - 8
	magic                       = "EKVPS001"
	beginKind             uint8 = 1
	replyKind             uint8 = 2
	finishKind            uint8 = 3
	ackKind               uint8 = 4
)

// StrictCodec preserves the ordinary proto codec for unrelated public messages.
// For Frame it rejects unknown/duplicate fields and noncanonical protobuf BEFORE
// cloning the body or allocating nested records. Native gRPC's shared receive
// envelope remains 64 MiB, so its wire buffer may already have been allocated.
type StrictCodec struct{}

func (StrictCodec) Name() string { return "proto" }
func (StrictCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, ErrFormat
	}
	if f, ok := v.(*wire.Frame); ok {
		if len(f.Body) == 0 || len(f.Body) > maxBody || len(f.ProtoReflect().GetUnknown()) != 0 {
			return nil, ErrFormat
		}
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(m)
	return b, errors.WithStack(err)
}

func (StrictCodec) Unmarshal(data []byte, v any) error {
	if frame, ok := v.(*wire.Frame); ok {
		body, err := strictBody(data)
		if err != nil {
			return err
		}
		frame.Reset()
		frame.Body = bytes.Clone(body)
		return nil
	}
	m, ok := v.(proto.Message)
	if !ok {
		return ErrFormat
	}
	return errors.WithStack(proto.Unmarshal(data, m))
}

func strictBody(data []byte) ([]byte, error) {
	if len(data) > MaxControlMessageSize {
		return nil, ErrFormat
	}
	number, typ, n := protowire.ConsumeTag(data)
	if n != 1 || number != 1 || typ != protowire.BytesType {
		return nil, ErrFormat
	}
	body, size := protowire.ConsumeBytes(data[n:])
	if size < 0 || n+size != len(data) || len(body) == 0 || len(body) > maxBody {
		return nil, ErrFormat
	}
	if size-len(body) != protowire.SizeVarint(uint64(len(body))) {
		return nil, ErrFormat
	}
	return body, nil
}

type view struct {
	Installed, Minimum    raftbootstrap.Versions
	Floor                 raftbootstrap.Floor
	Policy, Configuration raftbootstrap.Digest
	Generation            uint64
	State                 State
	MarkerReady           bool
}
type hello struct {
	Client, Server  Node
	Group           uint64
	Boot, Challenge Nonce
	View            view
}
type answer struct {
	Request         raftbootstrap.Digest
	Boot, Challenge Nonce
	View            view
}

type encoder struct {
	bytes.Buffer
	err error
}

func (e *encoder) fixed(v any) {
	if e.err == nil {
		e.err = errors.WithStack(binary.Write(&e.Buffer, binary.BigEndian, v))
	}
}
func (e *encoder) raw(b []byte) {
	if e.err != nil {
		return
	}
	if len(b) > maxBody-e.Len() {
		e.err = ErrFormat
		return
	}
	e.Write(b)
}
func (e *encoder) text(s string) {
	e.length(len(s))
	e.raw([]byte(s))
}
func (e *encoder) length(n int) {
	if n < 0 || n > maxBody {
		e.err = ErrFormat
		return
	}
	e.fixed(uint32(n))
}
func (e *encoder) node(n Node) {
	e.fixed(n.Cluster)
	e.fixed(n.Member)
	e.text(n.Address)
	e.fixed(n.Key)
}
func (e *encoder) result() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	if e.Len() > maxBody {
		return nil, ErrFormat
	}
	return e.Bytes(), nil
}
func newEncoder(kind uint8) *encoder { e := &encoder{}; e.WriteString(magic); e.fixed(kind); return e }

func encodeHello(h hello) ([]byte, error) {
	if !validNode(h.Client) || !validNode(h.Server) || h.Client.Cluster != h.Server.Cluster {
		return nil, ErrFormat
	}
	if h.Boot == (Nonce{}) || h.Challenge == (Nonce{}) || !validView(h.View) {
		return nil, ErrFormat
	}
	e := newEncoder(beginKind)
	e.node(h.Client)
	e.node(h.Server)
	e.fixed(h.Group)
	e.fixed(h.Boot)
	e.fixed(h.Challenge)
	e.fixed(h.View)
	return e.result()
}
func encodeAnswer(a answer) ([]byte, error) {
	if a.Request == (raftbootstrap.Digest{}) || a.Boot == (Nonce{}) || a.Challenge == (Nonce{}) || !validView(a.View) {
		return nil, ErrFormat
	}
	e := newEncoder(replyKind)
	e.fixed(a)
	return e.result()
}
func validView(v view) bool {
	if !validVersions(v.Installed) || !validVersions(v.Minimum) || !validState(v.State) {
		return false
	}
	return v.Generation != 0 && (v.Floor.ActivationIndex == 0) == (v.Floor.Generation == 0)
}
func proofFrame(kind uint8, session, proof raftbootstrap.Digest) *wire.Frame {
	e := newEncoder(kind)
	e.fixed(session)
	e.fixed(proof)
	return &wire.Frame{Body: e.Bytes()}
}

type decoder struct {
	*bytes.Reader
	err error
}

func (d *decoder) fixed(v any) {
	if d.err == nil {
		d.err = errors.WithStack(binary.Read(d.Reader, binary.BigEndian, v))
	}
}
func newDecoder(data []byte, kind uint8) (*decoder, error) {
	if len(data) < len(magic)+1 || len(data) > maxBody || string(data[:len(magic)]) != magic || data[len(magic)] != kind {
		return nil, ErrFormat
	}
	return &decoder{Reader: bytes.NewReader(data[len(magic)+1:])}, nil
}
func (d *decoder) text() string {
	var n uint32
	d.fixed(&n)
	if int64(n) > int64(d.Len()) {
		d.err = ErrFormat
		return ""
	}
	b := make([]byte, n)
	if d.err == nil {
		_, d.err = io.ReadFull(d.Reader, b)
	}
	return string(b)
}
func (d *decoder) node() Node {
	var n Node
	d.fixed(&n.Cluster)
	d.fixed(&n.Member)
	n.Address = d.text()
	d.fixed(&n.Key)
	return n
}
func (d *decoder) end() error {
	if d.err != nil || d.Len() != 0 {
		return ErrFormat
	}
	return nil
}

func parseHello(b []byte) (hello, error) {
	var h hello
	d, err := newDecoder(b, beginKind)
	if err != nil {
		return h, err
	}
	h.Client = d.node()
	h.Server = d.node()
	d.fixed(&h.Group)
	d.fixed(&h.Boot)
	d.fixed(&h.Challenge)
	d.fixed(&h.View)
	if err := d.end(); err != nil {
		return hello{}, err
	}
	canonical, err := encodeHello(h)
	if err != nil || !bytes.Equal(b, canonical) {
		return hello{}, ErrFormat
	}
	return h, nil
}
func parseAnswer(b []byte) (answer, error) {
	var a answer
	d, err := newDecoder(b, replyKind)
	if err != nil {
		return a, err
	}
	d.fixed(&a)
	if err := d.end(); err != nil {
		return answer{}, err
	}
	canonical, err := encodeAnswer(a)
	if err != nil || !bytes.Equal(b, canonical) {
		return answer{}, ErrFormat
	}
	return a, nil
}
func parseProof(b []byte, kind uint8) (raftbootstrap.Digest, raftbootstrap.Digest, error) {
	var id, proof raftbootstrap.Digest
	d, err := newDecoder(b, kind)
	if err != nil {
		return id, proof, err
	}
	d.fixed(&id)
	d.fixed(&proof)
	return id, proof, d.end()
}
func transcript(h, a []byte) raftbootstrap.Digest {
	e := &encoder{}
	e.length(len(h))
	e.raw(h)
	e.length(len(a))
	e.raw(a)
	return sha256.Sum256(e.Bytes())
}
