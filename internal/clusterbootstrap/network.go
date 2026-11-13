package clusterbootstrap

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"sort"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type bootstrapNetworkTarget struct {
	Group       uint64
	node        peersession.Node
	credentials credentials.TransportCredentials
}

// Ordinary connections have verified, address-pinned TLS but no peer session
// metadata and no startup credential. Public receiver admission owns authority.
// The control client's lifetime also owns ordinary call cancellation/join;
// only startup controls use its per-peer serialized Handle.Invoke path.
type BootstrapNetwork struct {
	Control *bootstrapControlClient
	targets map[string]bootstrapNetworkTarget
	Startup kv.StartupControlRPC
}

func NewBootstrapNetwork(ctx context.Context, domain *BootstrapDomain, configs map[uint64]*BootstrapPeerConfig) (*BootstrapNetwork, error) {
	control, err := newBootstrapControlClient(ctx, domain, configs)
	if err != nil {
		return nil, err
	}
	network := &BootstrapNetwork{Control: control, targets: make(map[string]bootstrapNetworkTarget)}
	ids := make([]uint64, 0, len(configs))
	for id := range configs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := network.addGroup(id, configs[id]); err != nil {
			control.Close()
			return nil, err
		}
	}
	network.Startup = kv.StartupOnlyControlRPC(control.Invoke, network.DataReady)
	return network, nil
}

func (n *BootstrapNetwork) addGroup(group uint64, config *BootstrapPeerConfig) error {
	local := n.Control.groups[group]
	for address, node := range local.peers {
		if prior, exists := n.targets[address]; exists {
			if prior.node != node {
				return errors.New("protected runtime has conflicting address identities")
			}
			continue
		}
		cfg := config.ClientTLS.Clone()
		priorVerify := cfg.VerifyConnection
		cfg.VerifyConnection = func(state tls.ConnectionState) error {
			if priorVerify != nil {
				if err := priorVerify(state); err != nil {
					return errors.WithStack(err)
				}
			}
			if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
				return errors.WithStack(peersession.ErrDenied)
			}
			if sha256.Sum256(state.PeerCertificates[0].RawSubjectPublicKeyInfo) != node.Key {
				return errors.WithStack(peersession.ErrDenied)
			}
			_, _, err := n.Control.target(group, address)
			return err
		}
		creds, err := local.owner.TransportCredentials(cfg, false)
		if err != nil {
			return errors.WithStack(err)
		}
		n.targets[address] = bootstrapNetworkTarget{Group: group, node: node, credentials: creds}
	}
	return nil
}

func (n *BootstrapNetwork) DataReady() bool {
	if n.Control.Ctx.Err() != nil {
		return false
	}
	for id, group := range n.Control.groups {
		if !bootstrapPublicReady(n.Control.Domain.Admission, group.owner, id) {
			return false
		}
	}
	return true
}

func (n *BootstrapNetwork) target(address string) (bootstrapNetworkTarget, error) {
	target, found := n.targets[address]
	if !found || n.Control.Ctx.Err() != nil {
		return target, errors.WithStack(peersession.ErrDenied)
	}
	_, _, err := n.Control.target(target.Group, address)
	return target, err
}

func (n *BootstrapNetwork) DialOptions(address string) ([]grpc.DialOption, error) {
	target, err := n.target(address)
	if err != nil {
		return nil, err
	}
	options := internalutil.GRPCApplicationDialOptions()
	return append(options, grpc.WithTransportCredentials(target.credentials.Clone()),
		grpc.WithChainUnaryInterceptor(n.Unary(address)), grpc.WithChainStreamInterceptor(n.Stream(address))), nil
}

func (n *BootstrapNetwork) Unary(address string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		owned, stop, err := n.Control.begin(ctx)
		if err != nil {
			return err
		}
		defer stop()
		if _, err := n.target(address); err != nil {
			return err
		}
		if err := invoke(owned, method, request, reply, conn, options...); err != nil {
			return errors.WithStack(err)
		}
		_, err = n.target(address)
		return bootstrapNetworkResult(err)
	}
}

func (n *BootstrapNetwork) Stream(address string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, create grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
		owned, stop, err := n.Control.begin(ctx)
		if err != nil {
			return nil, err
		}
		if _, err := n.target(address); err != nil {
			stop()
			return nil, err
		}
		stream, err := create(owned, desc, conn, method, options...)
		if err != nil {
			stop()
			return nil, errors.WithStack(err)
		}
		context.AfterFunc(stream.Context(), stop)
		return &bootstrapNetworkStream{ClientStream: stream, network: n, address: address}, nil
	}
}

type bootstrapNetworkStream struct {
	grpc.ClientStream
	network *BootstrapNetwork
	address string
}

func (s *bootstrapNetworkStream) SendMsg(message any) error {
	if _, err := s.network.target(s.address); err != nil {
		return err
	}
	return errors.WithStack(s.ClientStream.SendMsg(message))
}

func (s *bootstrapNetworkStream) RecvMsg(message any) error {
	if _, err := s.network.target(s.address); err != nil {
		return err
	}
	if err := s.ClientStream.RecvMsg(message); err != nil {
		return errors.WithStack(err)
	}
	_, err := s.network.target(s.address)
	return bootstrapNetworkResult(err)
}

func bootstrapNetworkResult(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(kv.ErrTransactionOutcomeUnknown, err)
}

func (n *BootstrapNetwork) Close() { n.Control.Close() }
