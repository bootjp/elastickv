package distribution

import (
    "fmt"
    "net/url"
    "testing"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/server/v3/embed"
)

func startEtcd(t *testing.T) (*embed.Etcd, *clientv3.Client) {
    cfg := embed.NewConfig()
    cfg.Logger = "zap"
    cfg.LogLevel = "error"
    cfg.Dir = t.TempDir()
    cfg.Name = "test"
    u1, _ := url.Parse("http://localhost:0")
    u2, _ := url.Parse("http://localhost:0")
    cfg.ListenClientUrls = []url.URL{*u1}
    cfg.AdvertiseClientUrls = cfg.ListenClientUrls
    cfg.ListenPeerUrls = []url.URL{*u2}
    cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
    cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.AdvertisePeerUrls[0].String())

    e, err := embed.StartEtcd(cfg)
    if err != nil {
        t.Fatalf("failed to start etcd: %v", err)
    }
    <-e.Server.ReadyNotify()

    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{e.Clients[0].Addr().String()},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        t.Fatalf("failed to create etcd client: %v", err)
    }
    return e, cli
}

// Test range statistics collection and split triggering.
func TestRangeStatsAndSplit(t *testing.T) {
    e, cli := startEtcd(t)
    defer e.Close()
    defer cli.Close()
    var notified bool
    eng := NewEngine(3, cli, func(left, right Range) {
        notified = true
    })

    eng.RecordRequest("a")
    eng.RecordRequest("a")

    stats := eng.GetStats()
    if len(stats) != 1 || stats[0].Count != 2 {
        t.Fatalf("expected single range count 2, got %+v", stats)
    }

    // Third request should trigger a split
    eng.RecordRequest("a")
    if !notified {
        t.Fatalf("expected split notification")
    }
    if len(eng.Ranges()) != 2 {
        t.Fatalf("expected 2 ranges after split, got %d", len(eng.Ranges()))
    }

    // Stats should now have two ranges with reset counts
    stats = eng.GetStats()
    if len(stats) != 2 {
        t.Fatalf("expected stats for 2 ranges, got %d", len(stats))
    }

    // find range containing "a"
    var count int
    for _, s := range stats {
        if (s.Start == "" || "a" >= s.Start) && (s.End == "" || "a" < s.End) {
            count = s.Count
        }
    }
    if count != 0 {
        t.Fatalf("expected count reset after split, got %d", count)
    }
}

// Ensure metadata updates after manual SplitRange call.
func TestManualSplitRange(t *testing.T) {
    e, cli := startEtcd(t)
    defer e.Close()
    defer cli.Close()
    eng := NewEngine(100, cli, nil)
    eng.SplitRange("a")
    if len(eng.Ranges()) != 2 {
        t.Fatalf("expected 2 ranges after manual split, got %d", len(eng.Ranges()))
    }
}

// Ensure timestamps persist and remain monotonic across restarts.
func TestTimestampPersistence(t *testing.T) {
    e, cli := startEtcd(t)
    defer e.Close()
    defer cli.Close()

    eng := NewEngine(3, cli, nil)
    eng.RecordRequest("a")
    ts1 := eng.GetStats()[0].Timestamp

    eng2 := NewEngine(3, cli, nil)
    eng2.RecordRequest("a")
    ts2 := eng2.GetStats()[0].Timestamp

    if ts2 <= ts1 {
        t.Fatalf("expected increasing timestamp, got %d <= %d", ts2, ts1)
    }
}

