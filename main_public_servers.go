package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func prepareRedisServer(ctx context.Context, lc *net.ListenConfig, redisAddr string, shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderRedis map[string]string, relay *adapter.RedisPubSubRelay, metricsRegistry *monitoring.Registry, readTracker *kv.ActiveTimestampTracker, redisApplyObserver *adapter.RedisApplyObserver) (*adapter.RedisServer, *adapter.DeltaCompactor, net.Listener, error) {
	redisL, err := lc.Listen(ctx, "tcp", redisAddr)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed to listen on %s", redisAddr)
	}
	deltaCompactor := newRedisDeltaCompactorIfEnabled(shardStore, coordinate, metricsRegistry)
	redisOpts := []adapter.RedisServerOption{
		adapter.WithRedisRequestAdmission(bootstrapRequestAdmissionForCoordinator(coordinate)),
		adapter.WithRedisActiveTimestampTracker(readTracker),
		adapter.WithRedisRequestObserver(metricsRegistry.RedisObserver()),
		adapter.WithLuaObserver(metricsRegistry.LuaObserver()),
		adapter.WithLuaFastPathObserver(metricsRegistry.LuaFastPathObserver()),
		adapter.WithLuaPoolMaxIdle(*redisLuaMaxIdleStates),
		adapter.WithRedisApplyObserver(redisApplyObserver),
	}
	if deltaCompactor != nil {
		redisOpts = append(redisOpts, adapter.WithRedisCompactor(deltaCompactor))
	}
	redisServer := adapter.NewRedisServer(redisL, redisAddr, shardStore, coordinate, leaderRedis, relay, redisOpts...)
	if dialOptions := shardStore.InternalGRPCDialOptions(); dialOptions != nil {
		if err := redisServer.SetInternalGRPCDialOptions(dialOptions); err != nil {
			redisServer.Stop()
			_ = redisL.Close()
			return nil, nil, nil, errors.WithStack(err)
		}
	}
	// Wire the bounded Lua VM pool into Prometheus. The metrics
	// (hits/misses/drops/idle/max_idle) are read at scrape time via
	// CounterFunc / GaugeFunc, so the EVAL hot path stays
	// observability-free. A registration error degrades observability
	// only — keep running and surface via slog so the operator can
	// notice on the next dashboard load rather than seeing a crash
	// loop here.
	if err := redisServer.RegisterLuaPoolMetrics(metricsRegistry.Registerer()); err != nil {
		slog.Warn("failed to register lua pool metrics; pool counters will be invisible in Prometheus", "err", err)
	}
	return redisServer, deltaCompactor, redisL, nil
}

func runRedisServer(ctx context.Context, eg *errgroup.Group, redisServer *adapter.RedisServer, deltaCompactor *adapter.DeltaCompactor) {
	if redisServer == nil {
		return
	}
	if deltaCompactor != nil {
		eg.Go(func() error { return deltaCompactor.Run(ctx) })
	}
	eg.Go(func() error {
		defer redisServer.Stop()
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				redisServer.Stop()
			case <-stop:
			}
		}()
		err := redisServer.Run()
		close(stop)
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	})
}

func newDynamoDBServer(shardStore *kv.ShardStore, coordinate kv.Coordinator, leaderDynamo map[string]string, metricsRegistry *monitoring.Registry, readTracker *kv.ActiveTimestampTracker) *adapter.DynamoDBServer {
	return adapter.NewDynamoDBServer(
		nil,
		shardStore,
		coordinate,
		adapter.WithDynamoDBActiveTimestampTracker(readTracker),
		adapter.WithDynamoDBRequestObserver(metricsRegistry.DynamoDBObserver()),
		adapter.WithDynamoDBLeaderMap(leaderDynamo),
		adapter.WithDynamoDBRequestAdmission(bootstrapRequestAdmissionForCoordinator(coordinate)),
	)
}

func bindDynamoDBServer(ctx context.Context, lc *net.ListenConfig, dynamoAddr string, dynamoServer *adapter.DynamoDBServer) (net.Listener, error) {
	if dynamoServer == nil {
		return nil, errors.New("dynamodb server is not prepared")
	}
	dynamoL, err := lc.Listen(ctx, "tcp", dynamoAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %s", dynamoAddr)
	}
	dynamoServer.SetListener(dynamoL)
	return dynamoL, nil
}

func runDynamoDBServer(ctx context.Context, eg *errgroup.Group, dynamoServer *adapter.DynamoDBServer) {
	eg.Go(func() error {
		defer dynamoServer.Stop()
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				dynamoServer.Stop()
			case <-stop:
			}
		}()
		err := dynamoServer.Run()
		close(stop)
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	})
}

func runPreparedPprofServer(ctx context.Context, eg *errgroup.Group, pprofServer *http.Server, pprofL net.Listener, pprofAddr string) {
	if pprofServer == nil || pprofL == nil {
		return
	}
	eg.Go(monitoring.PprofShutdownTask(ctx, pprofServer, pprofAddr))
	eg.Go(monitoring.PprofServeTask(pprofServer, pprofL, pprofAddr))
}

func runPreparedMetricsServer(ctx context.Context, eg *errgroup.Group, metricsServer *http.Server, metricsL net.Listener, metricsAddr string) {
	if metricsServer == nil || metricsL == nil {
		return
	}
	eg.Go(monitoring.MetricsShutdownTask(ctx, metricsServer, metricsAddr))
	eg.Go(monitoring.MetricsServeTask(metricsServer, metricsL, metricsAddr))
}
