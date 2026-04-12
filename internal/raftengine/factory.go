package raftengine

import "google.golang.org/grpc"

// FactoryConfig holds engine-agnostic parameters for creating a raft engine.
type FactoryConfig struct {
	LocalID      string
	LocalAddress string
	DataDir      string
	Peers        []Server
	Bootstrap    bool
	StateMachine StateMachine
}

// FactoryResult holds the output of Factory.Create.
type FactoryResult struct {
	Engine            Engine
	RegisterTransport func(grpc.ServiceRegistrar)
	// Close releases engine-specific resources that are not owned by
	// Engine.Close (e.g. raft log stores, transport managers). Callers
	// must call Engine.Close first to ensure the raft instance is fully
	// shut down before the underlying stores and transports are released.
	Close func() error
}

// Factory creates raft engine instances. Engine-specific configuration
// (timeouts, tick intervals, etc.) is provided at factory construction
// time; the Create method receives only engine-agnostic parameters.
type Factory interface {
	EngineType() string
	Create(cfg FactoryConfig) (*FactoryResult, error)
}
