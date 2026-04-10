package raftadmin

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultHealthPollInterval = 250 * time.Millisecond
	minHealthPollInterval     = 10 * time.Millisecond
	healthPollIntervalEnv     = "RAFTADMIN_HEALTH_POLL_INTERVAL_MS"
)

func RegisterOperationalServices(ctx context.Context, gs *grpc.Server, engine raftengine.Engine, serviceNames []string) {
	if gs == nil {
		return
	}

	pb.RegisterRaftAdminServer(gs, NewServer(engine))

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(gs, healthSrv)
	if ctx == nil {
		registerStaticHealthService(healthSrv, engine, serviceNames)
		return
	}
	go observeLeaderHealth(ctx, engine, healthSrv, serviceNames, healthPollInterval())
}

func registerStaticHealthService(healthSrv *health.Server, engine raftengine.Engine, serviceNames []string) {
	setHealthStatus(healthSrv, dedupeServices(serviceNames), currentHealthStatus(context.Background(), engine))
}

func observeLeaderHealth(ctx context.Context, engine raftengine.Engine, healthSrv *health.Server, serviceNames []string, pollInterval time.Duration) {
	services := dedupeServices(serviceNames)
	status := currentHealthStatus(ctx, engine)
	setHealthStatus(healthSrv, services, status)

	ticker := time.NewTicker(pollInterval)
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

func healthPollInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(healthPollIntervalEnv))
	if raw == "" {
		return defaultHealthPollInterval
	}
	millis, err := strconv.Atoi(raw)
	if err != nil || millis <= 0 {
		return defaultHealthPollInterval
	}
	interval := time.Duration(millis) * time.Millisecond
	if interval < minHealthPollInterval {
		return minHealthPollInterval
	}
	return interval
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
