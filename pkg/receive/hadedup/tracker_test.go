// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hadedup

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func testConfig() Config {
	return Config{
		ReplicaLabel:           "prometheus_replica",
		FailoverIntervals:      1.5,
		MinFailoverTimeout:     10 * time.Second,
		MaxFailoverTimeout:     5 * time.Minute,
		DefaultFailoverTimeout: time.Minute,
		MaxReplicas:            1024,
	}
}

type step struct {
	replica string
	ts      int64
	stale   bool
	accept  bool
}

func TestTrackerAccept(t *testing.T) {
	t.Parallel()

	const (
		a = "prometheus-0"
		b = "prometheus-1"
		c = "prometheus-2"
	)
	for _, tcase := range []struct {
		name              string
		steps             []step
		expectedOwner     string
		expectedFailovers float64
	}{
		{
			name:          "first sample elects its replica",
			steps:         []step{{replica: a, ts: 0, accept: true}},
			expectedOwner: a,
		},
		{
			name: "steady owner, other replica is dropped",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: b, ts: 5000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 20000, accept: false},
				{replica: a, ts: 30000, accept: true},
				{replica: b, ts: 35000, accept: false},
			},
			expectedOwner: a,
		},
		{
			name: "owner samples not newer than the last one are accepted",
			steps: []step{
				{replica: a, ts: 15000, accept: true},
				{replica: a, ts: 15000, accept: true},
				{replica: a, ts: 10000, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "failover just before the timeout",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 15000, accept: true},
				// Timeout is 1.5 * 15s = 22.5s.
				{replica: b, ts: 15000 + 22500 - 1, accept: false},
			},
			expectedOwner: a,
		},
		{
			name: "failover exactly at the timeout",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 15000 + 22500, accept: false},
			},
			expectedOwner: a,
		},
		{
			name: "failover just after the timeout",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 15000 + 22500 + 1, accept: true},
				{replica: a, ts: 45000, accept: false},
				{replica: b, ts: 52500, accept: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "default timeout until an interval is learned",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: b, ts: 60000, accept: false},
				{replica: b, ts: 60001, accept: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "min timeout clamp",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 1000, accept: true},
				// 1.5 * 1s is clamped to 10s.
				{replica: b, ts: 1000 + 10000, accept: false},
				{replica: b, ts: 1000 + 10000 + 1, accept: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "max timeout clamp",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 600000, accept: true},
				// 1.5 * 10m is clamped to 5m.
				{replica: b, ts: 600000 + 300000, accept: false},
				{replica: b, ts: 600000 + 300000 + 1, accept: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "gaps do not grow the learned interval",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 15000, accept: true},
				// A 60s gap is undone by the next regular sample, the interval stays 15s.
				{replica: a, ts: 75000, accept: true},
				{replica: a, ts: 90000, accept: true},
				{replica: b, ts: 90000 + 22500, accept: false},
				{replica: b, ts: 90000 + 22500 + 1, accept: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "smaller interval replaces the learned one",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 30000, accept: true},
				{replica: a, ts: 40000, accept: true},
				// Interval is now 10s, timeout 15s.
				{replica: b, ts: 40000 + 15000, accept: false},
				{replica: b, ts: 40000 + 15000 + 1, accept: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "stale marker of the owner is handed over to a live replica",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: b, ts: 5000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 20000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false},
				{replica: b, ts: 35000, accept: true},
				{replica: a, ts: 45000, accept: false},
				{replica: b, ts: 50000, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "samples of a lagging new owner older than the handed over ones are dropped",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 10000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false},
				// prometheus-0 already delivered samples up to 15s.
				{replica: b, ts: 12000, accept: false},
				{replica: b, ts: 15000, accept: false},
				{replica: b, ts: 25000, accept: true},
				{replica: b, ts: 20000, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "stale marker of the owner is accepted without a live replica",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 15000, accept: true},
				{replica: a, ts: 30000, stale: true, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "stale marker of the owner is accepted if the other replica went silent",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: b, ts: 5000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: a, ts: 30000, accept: true},
				{replica: a, ts: 45000, accept: true},
				// B was last seen at 5s, more than 22.5s before.
				{replica: a, ts: 60000, stale: true, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "stale on both replicas ends the series without ping-pong",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: b, ts: 5000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 20000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false},
				{replica: b, ts: 35000, stale: true, accept: true},
				{replica: a, ts: 45000, stale: true, accept: false},
			},
			expectedOwner: b,
		},
		{
			name: "stale on the non-owner first does not steal the owner's stale marker",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: b, ts: 5000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 20000, stale: true, accept: false},
				{replica: a, ts: 30000, stale: true, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "lagging owner loses the series to the current replica",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 100000, accept: true},
				{replica: a, ts: 30000, accept: false},
				{replica: a, ts: 45000, accept: false},
				{replica: b, ts: 115000, accept: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "three replicas",
			steps: []step{
				{replica: a, ts: 0, accept: true},
				{replica: b, ts: 5000, accept: false},
				{replica: c, ts: 10000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: c, ts: 25000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false},
				{replica: c, ts: 40000, accept: true},
				{replica: b, ts: 50000, accept: false},
			},
			expectedOwner: c,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			tr := NewTracker(testConfig(), prometheus.NewRegistry())
			for i, s := range tcase.steps {
				r, ok := tr.Replica(s.replica)
				require.True(t, ok)
				require.Equal(t, s.accept, tr.Accept(1, r, s.ts, s.stale), "step %d: %+v", i, s)
			}

			owner, ok := tr.Replica(tcase.expectedOwner)
			require.True(t, ok)
			require.True(t, tr.IsOwner(1, owner))
			require.Equal(t, tcase.expectedFailovers, promtest.ToFloat64(tr.metrics.failovers))
			require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.trackedSeries))
		})
	}
}

func TestTrackerInit(t *testing.T) {
	t.Parallel()

	tr := NewTracker(testConfig(), prometheus.NewRegistry())
	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")

	tr.Init(1, a, 0)
	// A concurrent writer of the other replica created the series too, the first owner is kept.
	tr.Init(1, b, 1000)
	require.True(t, tr.IsOwner(1, a))
	require.False(t, tr.IsOwner(1, b))
	require.False(t, tr.Accept(1, b, 2000, false))
	require.True(t, tr.Accept(1, a, 15000, false))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.trackedSeries))
}

func TestTrackerSeriesAreIndependent(t *testing.T) {
	t.Parallel()

	tr := NewTracker(testConfig(), prometheus.NewRegistry())
	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")

	require.True(t, tr.Accept(1, a, 0, false))
	require.True(t, tr.Accept(2, b, 0, false))
	require.False(t, tr.Accept(1, b, 1000, false))
	require.False(t, tr.Accept(2, a, 1000, false))
	require.True(t, tr.IsOwner(1, a))
	require.True(t, tr.IsOwner(2, b))
	require.True(t, tr.IsOwner(3, a), "untracked series has no owner to protect")
}

func TestTrackerReplicaTableOverflow(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxReplicas = 2
	tr := NewTracker(cfg, prometheus.NewRegistry())

	a, ok := tr.Replica("a")
	require.True(t, ok)
	b, ok := tr.Replica("b")
	require.True(t, ok)
	require.NotEqual(t, a, b)

	_, ok = tr.Replica("c")
	require.False(t, ok)

	again, ok := tr.Replica("a")
	require.True(t, ok)
	require.Equal(t, a, again)
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.replicas))
}

func newTestTracker(cfg Config, now *time.Time) *Tracker {
	tr := NewTracker(cfg, prometheus.NewRegistry())
	tr.now = func() time.Time { return *now }
	return tr
}

func TestTrackerGC(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.StateTTL = 5 * time.Minute
	cfg.DefaultFailoverTimeout = 5 * time.Minute
	now := time.UnixMilli(1_700_000_000_000)
	tr := newTestTracker(cfg, &now)
	require.Equal(t, 0, tr.GC())

	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")
	ts := now.UnixMilli()

	require.True(t, tr.Accept(1, a, ts-(6*time.Minute).Milliseconds(), false))
	require.True(t, tr.Accept(2, a, ts-(6*time.Minute).Milliseconds(), false))
	// Series 2 is kept alive by a non-owner sample.
	require.False(t, tr.Accept(2, b, ts-(4*time.Minute).Milliseconds(), false))
	require.True(t, tr.Accept(3, a, ts, false))

	require.Equal(t, 1, tr.GC())
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.trackedSeries))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.gcRemoved))

	// Removed state is re-created by the next sample of any replica.
	require.True(t, tr.Accept(1, b, ts, false))
	require.True(t, tr.IsOwner(1, b))

	// Series 2 was last seen 4m ago, which becomes more than 5m.
	now = now.Add(time.Minute + time.Millisecond)
	require.Equal(t, 1, tr.GC())
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.trackedSeries))
}

func TestTrackerGCIgnoresFutureSamples(t *testing.T) {
	t.Parallel()

	now := time.UnixMilli(1_700_000_000_000)
	tr := newTestTracker(testConfig(), &now)
	a, _ := tr.Replica("a")
	ts := now.UnixMilli()

	require.True(t, tr.Accept(1, a, ts, false))
	require.True(t, tr.Accept(2, a, ts+(365*24*time.Hour).Milliseconds(), false))
	require.Equal(t, 0, tr.GC())

	// The default TTL is max failover timeout + 10m.
	now = now.Add(15*time.Minute + time.Millisecond)
	require.Equal(t, 1, tr.GC())
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.trackedSeries))
}

func TestTrackerReplicaGC(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxReplicas = 2
	cfg.StateTTL = 5 * time.Minute
	now := time.UnixMilli(1_700_000_000_000)
	tr := newTestTracker(cfg, &now)
	ts := func() int64 { return now.UnixMilli() }

	a, ok := tr.Replica("a")
	require.True(t, ok)
	b, ok := tr.Replica("b")
	require.True(t, ok)
	require.True(t, tr.Accept(1, a, ts(), false))
	require.True(t, tr.Accept(2, b, ts(), false))
	tr.GC()

	_, ok = tr.Replica("c")
	require.False(t, ok, "replica table is full")

	// Only series 1 keeps receiving samples. No replica is looked up in the meantime, but a still owns series 1.
	for range 6 {
		now = now.Add(time.Minute)
		require.True(t, tr.Accept(1, a, ts(), false))
		tr.GC()
	}
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.replicas))

	c, ok := tr.Replica("c")
	require.True(t, ok)
	require.Equal(t, b, c, "freed index is reused")
	require.True(t, tr.Accept(3, c, ts(), false))
	require.False(t, tr.Accept(1, c, ts()+1000, false))
	require.True(t, tr.IsOwner(1, a))

	again, ok := tr.Replica("a")
	require.True(t, ok)
	require.Equal(t, a, again)
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.replicas))

	// Churning replica values keep being deduplicated once old ones age out.
	for i := range 5 {
		tr.GC()
		now = now.Add(6 * time.Minute)
		tr.GC()

		r0, ok := tr.Replica(fmt.Sprintf("churn-%d-0", i))
		require.True(t, ok, "iteration %d", i)
		r1, ok := tr.Replica(fmt.Sprintf("churn-%d-1", i))
		require.True(t, ok, "iteration %d", i)
		require.True(t, tr.Accept(storage.SeriesRef(100+i), r0, ts(), false))
		require.False(t, tr.Accept(storage.SeriesRef(100+i), r1, ts()+1000, false))
	}
}

func TestLearnInterval(t *testing.T) {
	t.Parallel()

	cur := learnInterval(0, 15000)
	require.Equal(t, uint32(15000), cur)

	// Occasional almost doubled deltas don't make the interval drift upwards.
	for i := range 100 {
		delta := int64(15000)
		if i%5 == 0 {
			delta = 29900
		}
		cur = learnInterval(cur, delta)
		require.LessOrEqual(t, cur, uint32(17000))
	}
	cur = learnInterval(cur, 15000)
	require.Equal(t, uint32(15000), cur)

	// A real interval change is learned within a bounded number of samples.
	var n int
	for cur < 29000 {
		cur = learnInterval(cur, 30000)
		n++
		require.LessOrEqual(t, n, 25)
	}
	require.LessOrEqual(t, cur, uint32(30000))

	require.Equal(t, uint32(10000), learnInterval(cur, 10000))
	require.Equal(t, uint32(10000), learnInterval(10000, 0))
	require.Equal(t, uint32(10000), learnInterval(10000, -5))
}

func TestTrackerConcurrent(t *testing.T) {
	t.Parallel()

	tr := NewTracker(testConfig(), prometheus.NewRegistry())

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			r, ok := tr.Replica(fmt.Sprintf("replica-%d", w%2))
			require.True(t, ok)
			for i := range 1000 {
				ts := int64(i * 15000)
				ref := storage.SeriesRef(i % 100)
				tr.Accept(ref, r, ts, false)
				tr.IsOwner(ref, r)
			}
		})
	}
	wg.Go(func() {
		for range 10 {
			tr.GC()
		}
	})
	wg.Wait()

	var tracked int
	for i := range tr.stripes {
		tracked += len(tr.stripes[i].series)
	}
	require.Equal(t, float64(tracked), promtest.ToFloat64(tr.metrics.trackedSeries))
	require.Equal(t, 4000.0, promtest.ToFloat64(tr.metrics.accepted)+promtest.ToFloat64(tr.metrics.dropped))
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		name  string
		cfg   func(c *Config)
		valid bool
	}{
		{name: "default", cfg: func(c *Config) {}, valid: true},
		{name: "no label", cfg: func(c *Config) { c.ReplicaLabel = "" }},
		{name: "reserved metric name label", cfg: func(c *Config) { c.ReplicaLabel = "__name__" }},
		{name: "reserved label prefix", cfg: func(c *Config) { c.ReplicaLabel = "__replica__" }},
		{name: "failover intervals equal to 1", cfg: func(c *Config) { c.FailoverIntervals = 1 }},
		{name: "min greater than max", cfg: func(c *Config) { c.MinFailoverTimeout = 10 * time.Minute }},
		{name: "zero default timeout", cfg: func(c *Config) { c.DefaultFailoverTimeout = 0 }},
		{name: "default timeout below min", cfg: func(c *Config) { c.DefaultFailoverTimeout = 5 * time.Second }},
		{name: "default timeout above max", cfg: func(c *Config) { c.DefaultFailoverTimeout = 6 * time.Minute }},
		{name: "default timeout equal to min and max", cfg: func(c *Config) {
			c.MinFailoverTimeout, c.DefaultFailoverTimeout, c.MaxFailoverTimeout = time.Minute, time.Minute, time.Minute
		}, valid: true},
		{name: "zero max replicas", cfg: func(c *Config) { c.MaxReplicas = 0 }},
		{name: "too many max replicas", cfg: func(c *Config) { c.MaxReplicas = MaxReplicasLimit + 1 }},
		{name: "negative state TTL", cfg: func(c *Config) { c.StateTTL = -time.Second }},
		{name: "state TTL below max failover timeout", cfg: func(c *Config) { c.StateTTL = 4 * time.Minute }},
		{name: "state TTL equal to max failover timeout", cfg: func(c *Config) { c.StateTTL = 5 * time.Minute }, valid: true},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			cfg := testConfig()
			tcase.cfg(&cfg)
			if tcase.valid {
				require.NoError(t, cfg.Validate())
			} else {
				require.Error(t, cfg.Validate())
			}
		})
	}
}

func BenchmarkTrackerAccept(b *testing.B) {
	tr := NewTracker(testConfig(), prometheus.NewRegistry())
	a, _ := tr.Replica("a")
	rb, _ := tr.Replica("b")

	const numSeries = 10000
	b.ReportAllocs()
	b.ResetTimer()
	var i int64
	for b.Loop() {
		ref := storage.SeriesRef(i % numSeries)
		ts := (i / numSeries) * 15000
		tr.Accept(ref, a, ts, false)
		tr.Accept(ref, rb, ts+5000, false)
		i++
	}
}
