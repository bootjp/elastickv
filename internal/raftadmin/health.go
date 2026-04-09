package raftadmin

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultHealthPollInterval = 250 * time.Millisecond
)

func RegisterOperationalServices(ctx context.Context, gs *grpc.Server, engine raftengine.Engine, serviceNames []string) {
	if gs == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	pb.RegisterRaftAdminServer(gs, NewServer(engine))

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(gs, healthSrv)
	go observeLeaderHealth(ctx, engine, healthSrv, serviceNames)
}

func observeLeaderHealth(ctx context.Context, engine raftengine.Engine, healthSrv *health.Server, serviceNames []string) {
	services := dedupeServices(serviceNames)
	status := currentHealthStatus(ctx, engine)
	setHealthStatus(healthSrv, services, status)

	ticker := time.NewTicker(defaultHealthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			setHealthStatus(healthSrv, services, healthpb.HealthCheckResponse_NOT_SERVING)
			return
		case <-ticker.C:
			next := currentHealthStatus(ctx, engine)
			if next != status {
				setHealthStatus(healthSrv, services, next)
				status = next
			}
		}
	}
}

func currentHealthStatus(parent context.Context, engine raftengine.Engine) healthpb.HealthCheckResponse_ServingStatus {
	if engine == nil {
		return healthpb.HealthCheckResponse_NOT_SERVING
	}
	checker, ok := any(engine).(raftengine.HealthReader)
	if !ok {
		return servingStatusForState(engine.State())
	}

	if err := checker.CheckServing(parent); err != nil {
		return healthpb.HealthCheckResponse_NOT_SERVING
	}
	return healthpb.HealthCheckResponse_SERVING
}

func servingStatusForState(state raftengine.State) healthpb.HealthCheckResponse_ServingStatus {
	if state == raftengine.StateLeader {
		return healthpb.HealthCheckResponse_SERVING
	}
	return healthpb.HealthCheckResponse_NOT_SERVING
}

func dedupeServices(serviceNames []string) []string {
	seen := map[string]struct{}{"": {}}
	services := []string{""}
	for _, name := range serviceNames {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		services = append(services, name)
	}
	return services
}

func setHealthStatus(healthSrv *health.Server, services []string, status healthpb.HealthCheckResponse_ServingStatus) {
	if healthSrv == nil {
		return
	}
	for _, service := range services {
		healthSrv.SetServingStatus(service, status)
	}
}
