package peersession

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
)

func controlFixture(t testing.TB) (hello, answer) {
	t.Helper()
	id, err := raftbootstrap.ParseUUID("0887e827-d4b1-4032-b9db-f71d120c04ef")
	require.NoError(t, err)
	v := view{Installed: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, Generation: 1, State: Intact, MarkerReady: true}
	h := hello{Client: Node{Cluster: id, Member: 2, Address: "client.test:9001", Key: sha256.Sum256([]byte("client"))}, Server: Node{Cluster: id, Member: 1, Address: "server.test:9000", Key: sha256.Sum256([]byte("server"))}, Group: testGroup, Boot: Nonce{1}, Challenge: Nonce{2}, View: v}
	b, err := encodeHello(h)
	require.NoError(t, err)
	return h, answer{Request: sha256.Sum256(b), Boot: Nonce{3}, Challenge: Nonce{4}, View: v}
}
func TestCanonicalControlRecordBoundaries(t *testing.T) {
	h, a := controlFixture(t)
	hb, err := encodeHello(h)
	require.NoError(t, err)
	ab, err := encodeAnswer(a)
	require.NoError(t, err)
	parsed, err := parseHello(hb)
	require.NoError(t, err)
	require.Equal(t, h, parsed)
	parsedAnswer, err := parseAnswer(ab)
	require.NoError(t, err)
	require.Equal(t, a, parsedAnswer)
	for n := 0; n < len(hb); n++ {
		_, err := parseHello(hb[:n])
		require.Error(t, err)
	}
	for n := 0; n < len(ab); n++ {
		_, err := parseAnswer(ab[:n])
		require.Error(t, err)
	}
	_, err = parseHello(append(bytes.Clone(hb), 0))
	require.Error(t, err)
	_, err = parseAnswer(append(bytes.Clone(ab), 0))
	require.Error(t, err)
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 'X' },
		func(b []byte) { b[len(magic)] = 99 },
		func(b []byte) { binary.BigEndian.PutUint32(b[len(magic)+1+16+8:], ^uint32(0)) },
		func(b []byte) { b[len(b)-1] = 2 },
	} {
		b := bytes.Clone(hb)
		mutate(b)
		_, err := parseHello(b)
		require.Error(t, err)
	}
	_, err = parseHello(make([]byte, maxBody+1))
	require.Error(t, err)
	for _, change := range []func(*hello){
		func(h *hello) { h.Client.Cluster = raftbootstrap.UUID{} }, func(h *hello) { h.Server.Cluster[0]++ }, func(h *hello) { h.Client.Member = 0 },
		func(h *hello) { h.Client.Address = "bad" }, func(h *hello) { h.Server.Key = raftbootstrap.Digest{} }, func(h *hello) { h.Boot = Nonce{} }, func(h *hello) { h.Challenge = Nonce{} },
		func(h *hello) { h.View.Installed.Session = 0 }, func(h *hello) { h.View.Installed.Entry = 2 }, func(h *hello) { h.View.State = State(99) }, func(h *hello) { h.View.Generation = 0 }, func(h *hello) { h.View.Floor.Generation = 1 },
	} {
		modified := h
		change(&modified)
		_, err := encodeHello(modified)
		require.Error(t, err)
	}
	key := raftbootstrap.Digest{9}
	id := transcript(hb, ab)
	require.NotEqual(t, proof(key, "client-finish", id), proof(key, "server-ack", id))
	p := proofFrame(finishKind, id, proof(key, "client-finish", id))
	got, mac, err := parseProof(p.Body, finishKind)
	require.NoError(t, err)
	require.Equal(t, id, got)
	require.Equal(t, proof(key, "client-finish", id), mac)
	for n := 0; n < len(p.Body); n++ {
		_, _, err := parseProof(p.Body[:n], finishKind)
		require.Error(t, err)
	}
}

func TestOwnerConfigAndZeroHandleDeny(t *testing.T) {
	h, _ := controlFixture(t)
	cfg := Config{Local: h.Client, Versions: h.View.Installed, Provisioned: []Provision{{Node: h.Client, Groups: []uint64{testGroup}}, {Node: h.Server, Groups: []uint64{testGroup}}}}
	o, err := NewOwner(cfg)
	require.NoError(t, err)
	o.Close()
	require.Equal(t, Counts{}, o.Counts())
	for _, change := range []func(*Config){
		func(c *Config) { c.Local.Member = 0 }, func(c *Config) { c.Versions.Session = 0 }, func(c *Config) { c.Provisioned = nil },
		func(c *Config) { c.Methods = []Method{{Name: sendMethod}} }, func(c *Config) { c.PublicMethods = []string{wire.PeerSession_Begin_FullMethodName} },
	} {
		copy := cfg
		change(&copy)
		_, err := NewOwner(copy)
		require.Error(t, err)
	}
	_, err = NewOwner(Config{Local: h.Client, Versions: h.View.Installed, Provisioned: append(cfg.Provisioned, cfg.Provisioned[1])})
	require.Error(t, err)
	var zero Handle
	require.Equal(t, raftbootstrap.Digest{}, zero.SessionID())
	require.Error(t, zero.CheckIncoming(t.Context(), sendMethod, request(Probe)))
	select {
	case <-zero.Done():
	default:
		t.Fatal("zero handle must be closed")
	}
	_, err = zero.GetRequestMetadata(t.Context())
	require.Error(t, err)
}

func FuzzControlRecords(f *testing.F) {
	h, a := controlFixture(f)
	hb, err := encodeHello(h)
	require.NoError(f, err)
	ab, err := encodeAnswer(a)
	require.NoError(f, err)
	f.Add(hb)
	f.Add(ab)
	f.Add(proofFrame(finishKind, raftbootstrap.Digest{1}, raftbootstrap.Digest{2}).Body)
	f.Fuzz(func(t *testing.T, b []byte) {
		if h, err := parseHello(b); err == nil {
			out, err := encodeHello(h)
			require.NoError(t, err)
			require.Equal(t, b, out)
		}
		if a, err := parseAnswer(b); err == nil {
			out, err := encodeAnswer(a)
			require.NoError(t, err)
			require.Equal(t, b, out)
		}
	})
}

func FuzzStrictControlEnvelope(f *testing.F) {
	b, err := (StrictCodec{}).Marshal(request(Probe))
	require.NoError(f, err)
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		var frame wire.Frame
		if (StrictCodec{}).Unmarshal(b, &frame) == nil {
			out, err := (StrictCodec{}).Marshal(&frame)
			require.NoError(t, err)
			require.Equal(t, b, out)
		}
	})
}
