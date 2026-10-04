// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hadedup

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
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

// testNow is the current time of tests, in milliseconds. Test samples are older unless they test the horizon.
const testNow = int64(1_700_000_000_000)

type step struct {
	// now advances the clock to the given time if set.
	now          int64
	replica      string
	ts           int64
	stale        bool
	accept       bool
	ownerChanged bool
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
		expectedFailovers int
		expectedTakeovers int
	}{
		{
			name:          "first sample elects its replica",
			steps:         []step{{replica: a, ts: 0, accept: true, ownerChanged: true}},
			expectedOwner: a,
		},
		{
			name: "steady owner, other replica is dropped",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
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
				{replica: a, ts: 15000, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				{replica: a, ts: 10000, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "failover just before the timeout",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				// Timeout is 1.5 * 15s = 22.5s.
				{replica: b, ts: 15000 + 22500 - 1, accept: false},
			},
			expectedOwner: a,
		},
		{
			name: "failover exactly at the timeout",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 15000 + 22500, accept: false},
			},
			expectedOwner: a,
		},
		{
			name: "failover just after the timeout",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 15000, accept: true},
				{replica: a, ts: 15000 + 22500 + 1, accept: true, ownerChanged: true},
				{replica: b, ts: 45000, accept: false},
				{replica: a, ts: 52500, accept: true},
			},
			expectedOwner:     a,
			expectedFailovers: 1,
		},
		{
			name: "default timeout until an interval is learned",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 60000, accept: false},
				{replica: b, ts: 60001, accept: true, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "min timeout clamp",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 1000, accept: true},
				// 1.5 * 1s is clamped to 10s.
				{replica: b, ts: 1000 + 10000, accept: false},
				{replica: b, ts: 1000 + 10000 + 1, accept: true, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "max timeout clamp",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 600000, accept: true},
				// 1.5 * 10m is clamped to 5m.
				{replica: b, ts: 600000 + 300000, accept: false},
				{replica: b, ts: 600000 + 300000 + 1, accept: true, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "gaps do not grow the learned interval",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				// A 60s gap is undone by the next regular sample, the interval stays 15s.
				{replica: a, ts: 75000, accept: true},
				{replica: a, ts: 90000, accept: true},
				{replica: b, ts: 90000 + 22500, accept: false},
				{replica: b, ts: 90000 + 22500 + 1, accept: true, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "smaller interval replaces the learned one",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 30000, accept: true},
				{replica: a, ts: 40000, accept: true},
				// Interval is now 10s, timeout 15s.
				{replica: b, ts: 40000 + 15000, accept: false},
				{replica: b, ts: 40000 + 15000 + 1, accept: true, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "stale marker of the owner is handed over to a live replica",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 5000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 20000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false, ownerChanged: true},
				{replica: b, ts: 35000, accept: true},
				{replica: a, ts: 34000, accept: false},
				{replica: b, ts: 50000, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "samples of a lagging new owner older than the handed over ones are dropped",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 10000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false, ownerChanged: true},
				// prometheus-0 already delivered samples up to 15s.
				{replica: b, ts: 12000, accept: false},
				{replica: b, ts: 15000, accept: false},
				{replica: b, ts: 25000, accept: true},
				{replica: b, ts: 20000, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "handover floor is kept when the old owner becomes candidate again",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 1000, accept: true},
				{replica: b, ts: 2000, accept: true},
				{replica: a, ts: 500, accept: false},
				{replica: b, ts: 3000, stale: true, accept: false, ownerChanged: true},
				{replica: b, ts: 4000, accept: false},
				{replica: a, ts: 1500, accept: false},
				{replica: a, ts: 2500, accept: true},
				{replica: a, ts: 1800, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "handover floor is kept when a third replica becomes candidate",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 1000, accept: true},
				{replica: a, ts: 2000, accept: true},
				{replica: b, ts: 500, accept: false},
				{replica: a, ts: 3000, stale: true, accept: false, ownerChanged: true},
				{replica: c, ts: 800, accept: false},
				{replica: b, ts: 1500, accept: false},
				{replica: b, ts: 2500, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "timestamps near the int64 limits don't overflow the timeout",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: math.MaxInt64 - 1000, accept: true},
				{replica: b, ts: 30000, accept: false},
				{replica: b, ts: math.MaxInt64, accept: false},
			},
			expectedOwner: a,
		},
		{
			name: "interval is not learned from the handover floor",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				{replica: a, ts: 30000, accept: true},
				{replica: b, ts: 29000, accept: false},
				{replica: a, ts: 45000, stale: true, accept: false, ownerChanged: true},
				{replica: b, ts: 31000, accept: true},
				{replica: b, ts: 46000, accept: true},
				{replica: c, ts: 57000, accept: false},
			},
			expectedOwner: b,
		},
		{
			name: "stale marker of the owner is accepted without a live replica",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				{replica: a, ts: 30000, stale: true, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "stale marker of the owner is accepted if the other replica went silent",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
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
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 5000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 20000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false, ownerChanged: true},
				{replica: b, ts: 35000, stale: true, accept: true},
				{replica: a, ts: 45000, stale: true, accept: false},
			},
			expectedOwner: b,
		},
		{
			name: "stale on the non-owner first does not steal the owner's stale marker",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
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
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 100000, accept: true, ownerChanged: true},
				{replica: a, ts: 30000, accept: false},
				{replica: a, ts: 45000, accept: false},
				{replica: b, ts: 115000, accept: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "sample from the future does not take the series over",
			steps: []step{
				{replica: a, ts: testNow - 15000, accept: true, ownerChanged: true},
				{replica: a, ts: testNow, accept: true},
				{replica: b, ts: testNow + time.Hour.Milliseconds(), accept: false},
				// Within the horizon, but only the time up to now counts as silence of the owner.
				{replica: b, ts: testNow + 4*time.Minute.Milliseconds(), accept: false},
				{replica: a, ts: testNow + 15000, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "sample from the future takes over once the owner is silent until now",
			steps: []step{
				{replica: a, ts: testNow - 15000, accept: true, ownerChanged: true},
				{replica: a, ts: testNow, accept: true},
				{now: testNow + 22500, replica: b, ts: testNow + 4*time.Minute.Milliseconds(), accept: false},
				{now: testNow + 22501, replica: b, ts: testNow + 4*time.Minute.Milliseconds() + 15000, accept: true, ownerChanged: true},
				{replica: a, ts: testNow + 30000, accept: false},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "owner sample from the future does not block a later failover",
			steps: []step{
				{replica: a, ts: testNow - 15000, accept: true, ownerChanged: true},
				{replica: a, ts: testNow, accept: true},
				// Appended as the owner's own data, but recorded at the horizon, 5m from now.
				{replica: a, ts: testNow + time.Hour.Milliseconds(), accept: true},
				{now: testNow + 5*time.Minute.Milliseconds() + 22500, replica: b, ts: testNow + 5*time.Minute.Milliseconds() + 22500, accept: false},
				// The learned interval is still 15s.
				{now: testNow + 5*time.Minute.Milliseconds() + 22501, replica: b, ts: testNow + 5*time.Minute.Milliseconds() + 22501, accept: true, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "sample from beyond the horizon takes over once the owner is silent until now",
			steps: []step{
				{replica: a, ts: testNow - 15000, accept: true, ownerChanged: true},
				{replica: a, ts: testNow, accept: true},
				// The owner stopped and the other replica's clock is an hour ahead.
				{now: testNow + 22500, replica: b, ts: testNow + time.Hour.Milliseconds(), accept: false},
				{now: testNow + 22501, replica: b, ts: testNow + time.Hour.Milliseconds() + 15000, accept: true, ownerChanged: true},
				// Recorded at the time of the failover, so the preferred old owner takes over with its next newer sample.
				{replica: a, ts: testNow + 22501, accept: false},
				{now: testNow + 22502, replica: a, ts: testNow + 22502, accept: true, ownerChanged: true},
			},
			expectedOwner:     a,
			expectedFailovers: 1,
			expectedTakeovers: 1,
		},
		{
			name: "sample from the future becomes standby at the current time",
			steps: []step{
				{replica: a, ts: testNow - 15000, accept: true, ownerChanged: true},
				{replica: a, ts: testNow, accept: true},
				{replica: b, ts: testNow + time.Hour.Milliseconds(), accept: false},
				{replica: a, ts: testNow + 15000, stale: true, accept: false, ownerChanged: true},
				// The handover floor is the current time, not the standby's timestamp.
				{replica: b, ts: testNow, accept: false},
				{replica: b, ts: testNow + 1, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "sample at the horizon becomes standby",
			steps: []step{
				{replica: a, ts: testNow - 15000, accept: true, ownerChanged: true},
				{replica: a, ts: testNow, accept: true},
				{replica: b, ts: testNow + 5*time.Minute.Milliseconds(), accept: false},
				{replica: a, ts: testNow + 15000, stale: true, accept: false, ownerChanged: true},
			},
			expectedOwner: b,
		},
		{
			name: "election by a sample from the future is recorded at the current time",
			steps: []step{
				{replica: a, ts: testNow + time.Hour.Milliseconds(), accept: true, ownerChanged: true},
				// No interval is learned yet, so the default timeout of 1m applies.
				{now: testNow + 60000, replica: b, ts: testNow + 60000, accept: false},
				{now: testNow + 60001, replica: b, ts: testNow + 60001, accept: true, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
		},
		{
			name: "owner elected with a clock ahead keeps its series",
			steps: []step{
				{replica: a, ts: testNow + 2000, accept: true, ownerChanged: true},
				{now: testNow + 15000, replica: a, ts: testNow + 17000, accept: true},
				{now: testNow + 30000, replica: a, ts: testNow + 32000, accept: true},
				{replica: b, ts: testNow + 30000, accept: false},
				{now: testNow + 45000, replica: a, ts: testNow + 47000, accept: true},
				{replica: b, ts: testNow + 45000, accept: false},
			},
			expectedOwner: a,
		},
		{
			name: "three replicas",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: c, ts: 5000, accept: false},
				{replica: b, ts: 10000, accept: false},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 25000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false, ownerChanged: true},
				{replica: b, ts: 40000, accept: true},
				{replica: c, ts: 50000, accept: false},
			},
			expectedOwner: b,
		},
		{
			name: "lagging third replica does not replace a live candidate",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 15000, accept: true},
				{replica: b, ts: 20000, accept: false},
				{replica: c, ts: -300000, accept: false},
				{replica: a, ts: 30000, stale: true, accept: false, ownerChanged: true},
				{replica: b, ts: 35000, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "preferred replica takes over from a live owner once it is current",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: -5000, accept: false},
				{replica: b, ts: 15000, accept: true},
				{replica: a, ts: 10000, accept: false},
				{replica: a, ts: 15000, accept: false},
				{replica: a, ts: 25000, accept: true, ownerChanged: true},
				{replica: b, ts: 30000, accept: false},
				{replica: a, ts: 40000, accept: true},
			},
			expectedOwner:     a,
			expectedTakeovers: 1,
		},
		{
			name: "lagging preferred replica stays standby",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 15000, accept: true},
				{replica: a, ts: 10000, accept: false},
				{replica: b, ts: 30000, accept: true},
				{replica: a, ts: 25000, accept: false},
				{replica: b, ts: 45000, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "stale marker of the preferred replica does not take over",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 5000, stale: true, accept: false},
				{replica: b, ts: 15000, accept: true},
			},
			expectedOwner: b,
		},
		{
			name: "takeover keeps the learned interval",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 15000, accept: true},
				{replica: a, ts: 20000, accept: true, ownerChanged: true},
				// The delta to the old owner's sample is not an interval, the timeout stays 22.5s.
				{replica: b, ts: 20000 + 22500, accept: false},
				{replica: b, ts: 20000 + 22500 + 1, accept: true, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedFailovers: 1,
			expectedTakeovers: 1,
		},
		{
			name: "preferred replica takes over from the owner of a handed over series",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: c, ts: 5000, accept: false},
				{replica: b, ts: 15000, stale: true, accept: false, ownerChanged: true},
				// Not newer than the handover floor of 5s.
				{replica: a, ts: 4000, accept: false},
				{replica: a, ts: 20000, accept: true, ownerChanged: true},
				{replica: c, ts: 25000, accept: false},
			},
			expectedOwner:     a,
			expectedTakeovers: 1,
		},
		{
			name: "stale marker of the preferred replica hands the series back to the live old owner",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 10000, accept: true},
				{replica: b, ts: 20000, accept: true},
				{replica: a, ts: 25000, accept: true, ownerChanged: true},
				{replica: a, ts: 30000, stale: true, accept: false, ownerChanged: true},
				{replica: b, ts: 30000, accept: true},
			},
			expectedOwner:     b,
			expectedTakeovers: 1,
		},
		{
			name: "takeover keeps the freshest third replica as standby",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 10000, accept: true},
				{replica: b, ts: 20000, accept: true},
				{replica: c, ts: 25000, accept: false},
				{replica: a, ts: 26000, accept: true, ownerChanged: true},
				{replica: a, ts: 36000, stale: true, accept: false, ownerChanged: true},
				{replica: c, ts: 35000, accept: true},
				{replica: c, ts: 45000, accept: true},
			},
			expectedOwner:     c,
			expectedTakeovers: 1,
		},
		{
			name: "takeover excludes an owner that accepted stale",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 10000, accept: true},
				{replica: b, ts: 20000, stale: true, accept: true},
				{replica: a, ts: 25000, accept: true, ownerChanged: true},
				{replica: a, ts: 35000, stale: true, accept: true},
			},
			expectedOwner:     a,
			expectedTakeovers: 1,
		},
		{
			name: "takeover excludes an owner elected with stale",
			steps: []step{
				{replica: b, ts: 0, stale: true, accept: true, ownerChanged: true},
				{replica: a, ts: 5000, accept: true, ownerChanged: true},
				{replica: a, ts: 10000, stale: true, accept: true},
			},
			expectedOwner:     a,
			expectedTakeovers: 1,
		},
		{
			name: "takeover excludes an owner failed over with stale",
			steps: []step{
				{replica: c, ts: 0, accept: true, ownerChanged: true},
				{replica: c, ts: 10000, accept: true},
				{replica: b, ts: 30000, stale: true, accept: true, ownerChanged: true},
				{replica: a, ts: 35000, accept: true, ownerChanged: true},
				{replica: a, ts: 40000, stale: true, accept: true},
			},
			expectedOwner:     a,
			expectedFailovers: 1,
			expectedTakeovers: 1,
		},
		{
			name: "new owner samples resume liveness after stale",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 10000, accept: true},
				{replica: b, ts: 20000, stale: true, accept: true},
				{replica: b, ts: 30000, accept: true},
				{replica: a, ts: 35000, accept: true, ownerChanged: true},
				{replica: a, ts: 40000, stale: true, accept: false, ownerChanged: true},
				{replica: b, ts: 45000, accept: true},
			},
			expectedOwner:     b,
			expectedTakeovers: 1,
		},
		{
			name: "older owner samples do not resume liveness after stale",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 10000, accept: true},
				{replica: b, ts: 20000, stale: true, accept: true},
				{replica: b, ts: 15000, accept: true},
				{replica: a, ts: 25000, accept: true, ownerChanged: true},
				{replica: a, ts: 35000, stale: true, accept: true},
			},
			expectedOwner:     a,
			expectedTakeovers: 1,
		},
		{
			name: "older stale owner samples do not end liveness",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 10000, accept: true},
				{replica: b, ts: 20000, accept: true},
				{replica: b, ts: 15000, stale: true, accept: true},
				{replica: a, ts: 25000, accept: true, ownerChanged: true},
				{replica: a, ts: 30000, stale: true, accept: false, ownerChanged: true},
			},
			expectedOwner:     b,
			expectedTakeovers: 1,
		},
		{
			name: "older stale marker does not hand over to standby",
			steps: []step{
				{replica: a, ts: 0, accept: true, ownerChanged: true},
				{replica: a, ts: 10000, accept: true},
				{replica: b, ts: 15000, accept: false},
				{replica: a, ts: 20000, accept: true},
				{replica: a, ts: 15000, stale: true, accept: true},
			},
			expectedOwner: a,
		},
		{
			name: "takeover of a handed over series has no standby",
			steps: []step{
				{replica: b, ts: 0, accept: true, ownerChanged: true},
				{replica: b, ts: 15000, accept: true},
				{replica: c, ts: 10000, accept: false},
				{replica: b, ts: 20000, stale: true, accept: false, ownerChanged: true},
				// The handover floor of 15s is the newest sample of prometheus-1, not of prometheus-2, which went silent.
				{replica: a, ts: 16000, accept: true, ownerChanged: true},
				{replica: a, ts: 36000, stale: true, accept: true},
			},
			expectedOwner:     a,
			expectedTakeovers: 1,
		},
		{
			name: "sample from the future of the preferred replica does not take over",
			steps: []step{
				{replica: b, ts: testNow - 15000, accept: true, ownerChanged: true},
				{replica: b, ts: testNow, accept: true},
				{now: testNow + 15000, replica: a, ts: testNow + 15001, accept: false},
				{replica: a, ts: testNow + 15000, accept: true, ownerChanged: true},
			},
			expectedOwner:     a,
			expectedTakeovers: 1,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			now := time.UnixMilli(testNow)
			tr := newTestTracker(testConfig(), &now)
			var failovers, takeovers int
			for i, s := range tcase.steps {
				if s.now != 0 {
					now = time.UnixMilli(s.now)
				}
				r, ok := tr.Replica(s.replica)
				require.True(t, ok)
				accept, change := tr.Accept(1, r, s.ts, tr.Horizon(), s.stale)
				require.Equal(t, s.accept, accept, "step %d: %+v", i, s)
				require.Equal(t, s.ownerChanged, change != NoChange, "step %d: %+v", i, s)
				switch change {
				case Failover:
					failovers++
				case Takeover:
					takeovers++
				}
			}

			owner, ok := tr.Replica(tcase.expectedOwner)
			require.True(t, ok)
			require.True(t, tr.IsOwner(1, owner))
			require.Equal(t, tcase.expectedFailovers, failovers)
			require.Equal(t, tcase.expectedTakeovers, takeovers)
			require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.trackedSeries))
		})
	}
}

func TestTrackerCopiesConverge(t *testing.T) {
	t.Parallel()

	// Two ingestors hold copies of a series. The second one was restarted and elected the replica whose sample arrived
	// first, while the other copy is owned by the preferred replica.
	now := time.UnixMilli(testNow)
	trackers := []*Tracker{newTestTracker(testConfig(), &now), newTestTracker(testConfig(), &now)}
	write := func(tr *Tracker, replica string, ts int64) bool {
		r, ok := tr.Replica(replica)
		require.True(t, ok)
		accept, _ := tr.Accept(1, r, ts, tr.Horizon(), false)
		return accept
	}
	require.True(t, write(trackers[0], "prometheus-0", 0))
	require.True(t, write(trackers[1], "prometheus-1", 7000))

	// The replicas scrape 7s apart, and every copy receives the samples of both.
	for i := range 4 {
		var accepted [2][]int64
		for j, tr := range trackers {
			for _, sample := range []struct {
				replica string
				ts      int64
			}{{"prometheus-0", int64(i+1) * 15000}, {"prometheus-1", int64(i+1)*15000 + 7000}} {
				if write(tr, sample.replica, sample.ts) {
					accepted[j] = append(accepted[j], sample.ts)
				}
			}
		}
		require.Equal(t, accepted[0], accepted[1], "copies diverge after %d scrapes", i+1)
	}
}

func TestLater(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		ts, last, d int64
		expected    bool
	}{
		{ts: 20001, last: 10000, d: 10000, expected: true},
		{ts: 20000, last: 10000, d: 10000, expected: false},
		{ts: 5000, last: 10000, d: 10000, expected: false},
		{ts: 30000, last: math.MaxInt64 - 1000, d: 10000, expected: false},
		{ts: math.MaxInt64, last: math.MaxInt64 - 1000, d: 10000, expected: false},
		{ts: math.MaxInt64, last: math.MinInt64, d: 10000, expected: true},
		{ts: math.MinInt64 + 1000, last: math.MinInt64, d: 10000, expected: false},
	} {
		require.Equal(t, tcase.expected, later(tcase.ts, tcase.last, tcase.d), "%+v", tcase)
	}
}

func acceptOnly(tr *Tracker, ref storage.SeriesRef, replica uint16, ts int64) bool {
	accept, _ := tr.Accept(ref, replica, ts, tr.Horizon(), false)
	return accept
}

func TestTrackerInit(t *testing.T) {
	t.Parallel()

	tr := NewTracker(log.NewNopLogger(), testConfig(), prometheus.NewRegistry())
	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")

	require.True(t, tr.Init(1, a, 0, tr.Horizon(), false))
	// A concurrent writer of the other replica created the series too, the first owner is kept.
	require.False(t, tr.Init(1, b, 1000, tr.Horizon(), false))
	require.True(t, tr.IsOwner(1, a))
	require.False(t, tr.IsOwner(1, b))
	require.False(t, acceptOnly(tr, 1, b, 2000))
	require.True(t, acceptOnly(tr, 1, a, 15000))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.trackedSeries))

	// The first sample of a new owner is recorded at most at the current time.
	h := tr.Horizon()
	require.True(t, tr.Init(2, b, h.max+1, h, false))
	require.Equal(t, h.now, trackedState(tr, 2).ownerLastTs)
	require.True(t, tr.Init(3, b, h.now-1, h, false))
	require.Equal(t, h.now-1, trackedState(tr, 3).ownerLastTs)
	require.Equal(t, 3.0, promtest.ToFloat64(tr.metrics.trackedSeries))

	require.True(t, tr.Init(4, b, 0, h, true))
	require.NotZero(t, trackedState(tr, 4).intervalMs&staleFlag)
	require.True(t, acceptOnly(tr, 4, a, 5000))
	accept, change := tr.Accept(4, a, 10000, h, true)
	require.True(t, accept)
	require.Equal(t, NoChange, change)
}

func trackedState(tr *Tracker, ref storage.SeriesRef) seriesState {
	s := &tr.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.series[ref]
}

func TestTrackerHorizon(t *testing.T) {
	t.Parallel()

	now := time.UnixMilli(testNow)
	tr := newTestTracker(testConfig(), &now)
	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")
	h := tr.Horizon()
	require.Equal(t, Horizon{now: testNow, max: testNow + (5 * time.Minute).Milliseconds()}, h)

	require.True(t, acceptOnly(tr, 1, a, h.now))
	require.True(t, acceptOnly(tr, 1, a, h.max-15000))
	require.True(t, acceptOnly(tr, 1, a, h.max))
	st := trackedState(tr, 1)
	require.Equal(t, h.max, st.ownerLastTs)
	require.Equal(t, uint32(15000), st.interval())

	// The owner's samples beyond the horizon are accepted, but neither advance its last sample nor the interval.
	require.True(t, acceptOnly(tr, 1, a, h.max+1))
	require.True(t, acceptOnly(tr, 1, a, h.max+time.Hour.Milliseconds()))
	require.Equal(t, st, trackedState(tr, 1))

	now = now.Add(time.Minute)
	require.True(t, acceptOnly(tr, 1, a, h.max+time.Hour.Milliseconds()))
	st.ownerLastTs = tr.Horizon().max
	require.Equal(t, st, trackedState(tr, 1))

	// The handover floor is bounded by the horizon too.
	require.True(t, acceptOnly(tr, 2, a, testNow))
	require.False(t, acceptOnly(tr, 2, b, testNow+5000))
	accept, change := tr.Accept(2, a, testNow+15000, tr.Horizon(), true)
	require.False(t, accept)
	require.Equal(t, Handover, change)
	require.True(t, acceptOnly(tr, 2, b, tr.Horizon().max+1))
	st = trackedState(tr, 2)
	require.Equal(t, tr.Horizon().max, st.ownerLastTs)
	require.Zero(t, st.intervalMs&handoverFlag)

	// Standby samples are recorded at most at the current time.
	require.False(t, acceptOnly(tr, 1, b, h.max+time.Hour.Milliseconds()))
	st = trackedState(tr, 1)
	require.Equal(t, b, st.cand)
	require.Equal(t, tr.Horizon().now, st.candLastTs)
}

func TestTrackerTimeout(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.FailoverIntervals = math.MaxFloat64
	tr := NewTracker(log.NewNopLogger(), cfg, prometheus.NewRegistry())
	require.Equal(t, cfg.MaxFailoverTimeout.Milliseconds(), tr.timeout(15000))
	require.Equal(t, cfg.MaxFailoverTimeout.Milliseconds(), tr.timeout(maxIntervalMs))
	require.Equal(t, cfg.DefaultFailoverTimeout.Milliseconds(), tr.timeout(0))
}

func TestTrackerForget(t *testing.T) {
	t.Parallel()

	tr := NewTracker(log.NewNopLogger(), testConfig(), prometheus.NewRegistry())
	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")
	c, _ := tr.Replica("c")
	require.True(t, acceptOnly(tr, 1, a, 0))
	require.True(t, acceptOnly(tr, 1, a, 15000))
	require.True(t, acceptOnly(tr, 2, a, 0))
	require.False(t, acceptOnly(tr, 1, b, 20000))

	require.True(t, tr.Forget(1))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.trackedSeries))
	require.False(t, tr.Tracked(1))
	require.True(t, tr.Tracked(2))
	require.True(t, tr.IsOwner(1, b), "forgotten series has no owner to protect")
	require.True(t, tr.IsOwner(2, a))
	require.False(t, tr.IsOwner(2, b))

	require.False(t, tr.Forget(1))
	require.False(t, tr.Forget(3))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.trackedSeries))

	// The next sample elects its replica, also one older than the forgotten owner's samples.
	accepted, change := tr.Accept(1, b, 10000, tr.Horizon(), false)
	require.True(t, accepted)
	require.Equal(t, Elected, change)
	require.True(t, tr.IsOwner(1, b))
	require.False(t, acceptOnly(tr, 1, c, 30000))
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.trackedSeries))
}

func TestSeriesStateSize(t *testing.T) {
	t.Parallel()

	require.Equal(t, uintptr(24), unsafe.Sizeof(seriesState{}))
}

func TestTrackerSeriesAreIndependent(t *testing.T) {
	t.Parallel()

	tr := NewTracker(log.NewNopLogger(), testConfig(), prometheus.NewRegistry())
	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")

	require.True(t, acceptOnly(tr, 1, a, 0))
	require.True(t, acceptOnly(tr, 2, b, 0))
	require.False(t, acceptOnly(tr, 1, b, 1000))
	require.False(t, acceptOnly(tr, 2, a, 0))
	require.True(t, tr.IsOwner(1, a))
	require.True(t, tr.IsOwner(2, b))
	require.True(t, tr.IsOwner(3, a), "untracked series has no owner to protect")
}

func TestTrackerReplicaTableOverflow(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxReplicas = 2
	tr := NewTracker(log.NewNopLogger(), cfg, prometheus.NewRegistry())

	a, ok := tr.Replica("a")
	require.True(t, ok)
	b, ok := tr.Replica("b")
	require.True(t, ok)
	require.NotEqual(t, a, b)

	_, ok = tr.Replica("c")
	require.False(t, ok)
	_, ok, full := tr.LookupReplica("c")
	require.False(t, ok)
	require.True(t, full)

	again, ok := tr.Replica("a")
	require.True(t, ok)
	require.Equal(t, a, again)
	again, ok, full = tr.LookupReplica("b")
	require.True(t, ok)
	require.False(t, full)
	require.Equal(t, b, again)
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.replicas))
}

func TestTrackerLookupReplica(t *testing.T) {
	t.Parallel()

	tr := NewTracker(log.NewNopLogger(), testConfig(), prometheus.NewRegistry())
	_, ok, full := tr.LookupReplica("a")
	require.False(t, ok)
	require.False(t, full)
	require.Zero(t, promtest.ToFloat64(tr.metrics.replicas), "lookups must not intern")

	a, ok := tr.Replica("a")
	require.True(t, ok)
	r, ok, _ := tr.LookupReplica("a")
	require.True(t, ok)
	require.Equal(t, a, r)
}

func TestTrackerFullReplicaTableLookupsShareTheLock(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxReplicas = 1
	tr := NewTracker(log.NewNopLogger(), cfg, prometheus.NewRegistry())
	_, ok := tr.Replica("a")
	require.True(t, ok)

	// Lookups of unknown values in a full table must not wait for the exclusive lock, so that a flood of junk values
	// doesn't serialize the writers of a tenant.
	tr.replicasMtx.RLock()
	defer tr.replicasMtx.RUnlock()
	done := make(chan bool)
	go func() {
		_, ok := tr.Replica("b")
		done <- ok
	}()
	select {
	case ok := <-done:
		require.False(t, ok)
	case <-time.After(10 * time.Second):
		t.Fatal("lookup of an unknown value in a full replica table took the exclusive lock")
	}
}

func TestTrackerLogs(t *testing.T) {
	t.Parallel()

	var buf syncBuffer
	tr := NewTracker(log.NewLogfmtLogger(&buf), testConfig(), prometheus.NewRegistry())
	_, ok := tr.Replica("prometheus-0")
	require.True(t, ok)
	_, ok = tr.Replica("prometheus-0")
	require.True(t, ok)
	require.Equal(t, "level=info msg=\"tracking new HA replica\" replica=prometheus-0 replicas=1\n", buf.reset())

	tr.Record(Counts{ReplicaTableFull: 2})
	tr.Record(Counts{ReplicaTableFull: 1, ReplicaValueTooLong: 4})
	tr.GC()
	require.Equal(t, "level=warn msg=\"HA replica table is full, series of further replicas are written without deduplication\" series=3 maxReplicas=1024\n"+
		"level=warn msg=\"HA replica label values are too long, their series are written without deduplication\" series=4 maxLength=128\n", buf.reset())

	// The warnings are logged at most once per GC.
	tr.GC()
	require.Empty(t, buf.reset())
}

type syncBuffer struct {
	mtx sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) reset() string {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	s := b.buf.String()
	b.buf.Reset()
	return s
}

func newTestTracker(cfg Config, now *time.Time) *Tracker {
	tr := NewTracker(log.NewNopLogger(), cfg, prometheus.NewRegistry())
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

	require.True(t, acceptOnly(tr, 1, a, ts-(6*time.Minute).Milliseconds()))
	require.True(t, acceptOnly(tr, 2, a, ts-(6*time.Minute).Milliseconds()))
	// Series 2 is kept alive by a non-owner sample.
	require.False(t, acceptOnly(tr, 2, b, ts-(4*time.Minute).Milliseconds()))
	require.True(t, acceptOnly(tr, 3, a, ts))

	require.Equal(t, 1, tr.GC())
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.trackedSeries))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.gcRemoved))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.seriesWithStandby))
	require.Positive(t, promtest.ToFloat64(tr.metrics.gcDuration))

	// Removed state is re-created by the next sample of any replica.
	require.True(t, acceptOnly(tr, 1, b, ts))
	require.True(t, tr.IsOwner(1, b))

	// Series 2 was last seen 4m ago, which becomes more than 5m.
	now = now.Add(time.Minute + time.Millisecond)
	require.Equal(t, 1, tr.GC())
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.trackedSeries))
	require.Zero(t, promtest.ToFloat64(tr.metrics.seriesWithStandby))
}

func TestTrackerGCRemovesDeadStandby(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxReplicas = 2
	cfg.StateTTL = 5 * time.Minute
	now := time.UnixMilli(testNow)
	tr := newTestTracker(cfg, &now)
	ts := func() int64 { return now.UnixMilli() }

	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")
	require.True(t, acceptOnly(tr, 1, a, ts()))
	require.False(t, acceptOnly(tr, 1, b, ts()+1000))
	tr.GC()
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.seriesWithStandby))

	// Only a keeps writing the series, b stopped and must not keep its slot of the replica table.
	for range 6 {
		now = now.Add(time.Minute)
		require.True(t, acceptOnly(tr, 1, a, ts()))
		tr.GC()
	}
	require.Equal(t, uint16(noReplica), trackedState(tr, 1).cand)
	require.Zero(t, promtest.ToFloat64(tr.metrics.seriesWithStandby))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.replicas))
	_, ok := tr.Replica("c")
	require.True(t, ok)
}

func TestTrackerStaleHandoverToFutureStandby(t *testing.T) {
	t.Parallel()

	now := time.UnixMilli(testNow)
	tr := newTestTracker(testConfig(), &now)
	r, _ := tr.Replica("real")
	m, _ := tr.Replica("made-up")

	for i := range 3 {
		now = now.Add(15 * time.Second)
		require.True(t, acceptOnly(tr, 1, r, testNow+int64(i+1)*15000))
	}
	require.False(t, acceptOnly(tr, 1, m, now.UnixMilli()+time.Hour.Milliseconds()))
	accept, change := tr.Accept(1, r, now.UnixMilli(), tr.Horizon(), true)
	require.False(t, accept)
	require.Equal(t, Handover, change)

	// The made-up replica doesn't write the series, so the real one takes it back after the failover timeout of the
	// 15s interval, instead of after the timestamp of the made-up replica's sample. The made-up replica is preferred,
	// so the real one can't take the series back earlier.
	now = now.Add(20 * time.Second)
	require.False(t, acceptOnly(tr, 1, r, now.UnixMilli()))
	now = now.Add(5 * time.Second)
	accept, change = tr.Accept(1, r, now.UnixMilli(), tr.Horizon(), false)
	require.True(t, accept)
	require.Equal(t, Failover, change)
}

func TestTrackerGCRemovesFutureState(t *testing.T) {
	t.Parallel()

	now := time.UnixMilli(testNow)
	tr := newTestTracker(testConfig(), &now)
	a, _ := tr.Replica("a")
	b, _ := tr.Replica("b")

	require.True(t, acceptOnly(tr, 1, a, testNow))
	// The clock is corrected by a day, the state of series 1 is now further ahead than any decision can store.
	now = now.Add(-24 * time.Hour)
	ts := now.UnixMilli()
	require.True(t, acceptOnly(tr, 2, a, ts))
	require.True(t, acceptOnly(tr, 3, a, ts))
	// Series 3 has a standby replica, whose sample from the future is recorded at the current time.
	require.False(t, acceptOnly(tr, 3, b, ts+(5*time.Minute).Milliseconds()))

	require.Equal(t, 1, tr.GC())
	require.True(t, tr.IsOwner(1, b))
	require.False(t, tr.IsOwner(2, b))
	require.False(t, tr.IsOwner(3, b))
	require.Equal(t, 2.0, promtest.ToFloat64(tr.metrics.trackedSeries))
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.seriesWithStandby))

	// The default TTL is max failover timeout + 10m, so both remaining series expire together.
	now = now.Add(15*time.Minute + time.Millisecond)
	require.Equal(t, 2, tr.GC())
	require.Zero(t, promtest.ToFloat64(tr.metrics.trackedSeries))
	require.Zero(t, promtest.ToFloat64(tr.metrics.seriesWithStandby))
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
	require.True(t, acceptOnly(tr, 1, a, ts()))
	require.True(t, acceptOnly(tr, 2, b, ts()))
	tr.GC()

	_, ok = tr.Replica("c")
	require.False(t, ok, "replica table is full")

	// Only series 1 keeps receiving samples. No replica is looked up in the meantime, but a still owns series 1.
	for range 6 {
		now = now.Add(time.Minute)
		require.True(t, acceptOnly(tr, 1, a, ts()))
		tr.GC()
	}
	require.Equal(t, 1.0, promtest.ToFloat64(tr.metrics.replicas))

	c, ok := tr.Replica("c")
	require.True(t, ok)
	require.Equal(t, b, c, "freed index is reused")
	require.True(t, acceptOnly(tr, 3, c, ts()))
	require.False(t, acceptOnly(tr, 1, c, ts()+1000))
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
		require.True(t, acceptOnly(tr, storage.SeriesRef(100+i), r0, ts()))
		require.False(t, acceptOnly(tr, storage.SeriesRef(100+i), r1, ts()+1000))
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

	// The top two bits are reserved for handoverFlag and staleFlag.
	require.Equal(t, uint32(10000), learnInterval(10000, maxIntervalMs+1))
	require.Equal(t, uint32(maxIntervalMs), learnInterval(maxIntervalMs-1, maxIntervalMs))
	require.Zero(t, learnInterval(maxIntervalMs/2+1, maxIntervalMs)&(handoverFlag|staleFlag))
}

func TestTrackerConcurrent(t *testing.T) {
	t.Parallel()

	tr := NewTracker(log.NewNopLogger(), testConfig(), prometheus.NewRegistry())

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			r, ok := tr.Replica(fmt.Sprintf("replica-%d", w%2))
			require.True(t, ok)
			var c Counts
			for i := range 1000 {
				ts := int64(i * 15000)
				ref := storage.SeriesRef(i % 100)
				if accept, _ := tr.Accept(ref, r, ts, tr.Horizon(), i%7 == 0); accept {
					c.Accepted++
				} else {
					c.Dropped++
				}
				tr.IsOwner(ref, r)
				if i%100 == 99 {
					tr.Record(c)
					c = Counts{}
				}
			}
		})
	}
	wg.Go(func() {
		r, ok := tr.Replica("replica-2")
		require.True(t, ok)
		for i := range 1000 {
			tr.Init(storage.SeriesRef(i%100), r, int64(i*15000), tr.Horizon(), false)
		}
		tr.Record(Counts{Accepted: 1000})
	})
	wg.Go(func() {
		for i := range 1000 {
			tr.Forget(storage.SeriesRef(i % 100))
		}
	})
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
	require.Equal(t, 5000.0, promtest.ToFloat64(tr.metrics.accepted)+promtest.ToFloat64(tr.metrics.dropped))
}

func TestTrackerRecord(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	tr := NewTracker(log.NewNopLogger(), testConfig(), reg)
	tr.Record(Counts{})
	tr.Record(Counts{Accepted: 3, Dropped: 2, Failovers: 1, NoLabel: 4, Elections: 2})
	tr.Record(Counts{Accepted: 1, ReplicaTableFull: 5, ReplicaValueTooLong: 6, Handovers: 7, Forgotten: 8, Takeovers: 9})

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
# HELP thanos_receive_ha_dedup_elections_total Total number of series whose owning HA replica was elected by their first tracked sample.
# TYPE thanos_receive_ha_dedup_elections_total counter
thanos_receive_ha_dedup_elections_total 2
# HELP thanos_receive_ha_dedup_failovers_total Total number of series taken over by another HA replica after the owning replica stopped writing them.
# TYPE thanos_receive_ha_dedup_failovers_total counter
thanos_receive_ha_dedup_failovers_total 1
# HELP thanos_receive_ha_dedup_forgotten_total Total number of series states removed because the samples they were based on were not stored.
# TYPE thanos_receive_ha_dedup_forgotten_total counter
thanos_receive_ha_dedup_forgotten_total 8
# HELP thanos_receive_ha_dedup_handovers_total Total number of series handed over to another HA replica after the owning replica wrote a stale marker.
# TYPE thanos_receive_ha_dedup_handovers_total counter
thanos_receive_ha_dedup_handovers_total 7
# HELP thanos_receive_ha_dedup_passthrough_total Total number of series written without HA deduplication, by reason.
# TYPE thanos_receive_ha_dedup_passthrough_total counter
thanos_receive_ha_dedup_passthrough_total{reason="no_label"} 4
thanos_receive_ha_dedup_passthrough_total{reason="replica_table_full"} 5
thanos_receive_ha_dedup_passthrough_total{reason="replica_value_too_long"} 6
# HELP thanos_receive_ha_dedup_samples_total Total number of samples from HA replicas processed by deduplication, by outcome.
# TYPE thanos_receive_ha_dedup_samples_total counter
thanos_receive_ha_dedup_samples_total{outcome="accepted"} 4
thanos_receive_ha_dedup_samples_total{outcome="dropped"} 2
# HELP thanos_receive_ha_dedup_takeovers_total Total number of series taken over from a live owning HA replica by the preferred replica, the one with the lowest replica label value.
# TYPE thanos_receive_ha_dedup_takeovers_total counter
thanos_receive_ha_dedup_takeovers_total 9
`), "thanos_receive_ha_dedup_elections_total", "thanos_receive_ha_dedup_failovers_total", "thanos_receive_ha_dedup_forgotten_total",
		"thanos_receive_ha_dedup_handovers_total", "thanos_receive_ha_dedup_passthrough_total", "thanos_receive_ha_dedup_samples_total",
		"thanos_receive_ha_dedup_takeovers_total"))
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
		{name: "NaN failover intervals", cfg: func(c *Config) { c.FailoverIntervals = math.NaN() }},
		{name: "infinite failover intervals", cfg: func(c *Config) { c.FailoverIntervals = math.Inf(1) }},
		{name: "negative infinite failover intervals", cfg: func(c *Config) { c.FailoverIntervals = math.Inf(-1) }},
		{name: "large failover intervals", cfg: func(c *Config) { c.FailoverIntervals = math.MaxFloat64 }, valid: true},
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
	tr := NewTracker(log.NewNopLogger(), testConfig(), prometheus.NewRegistry())
	a, _ := tr.Replica("a")
	rb, _ := tr.Replica("b")

	const numSeries = 10000
	h := tr.Horizon()
	b.ReportAllocs()
	b.ResetTimer()
	var i int64
	for b.Loop() {
		ref := storage.SeriesRef(i % numSeries)
		ts := (i / numSeries) * 15000
		tr.Accept(ref, a, ts, h, false)
		tr.Accept(ref, rb, ts+5000, h, false)
		i++
	}
}

func BenchmarkTrackerAcceptParallel(b *testing.B) {
	tr := NewTracker(log.NewNopLogger(), testConfig(), prometheus.NewRegistry())
	a, _ := tr.Replica("a")
	rb, _ := tr.Replica("b")

	const numSeries = 100000
	h := tr.Horizon()
	var worker atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every goroutine writes its own series, like concurrent requests of different targets of a tenant.
		base := worker.Add(1) * numSeries
		var i int64
		for pb.Next() {
			ref := storage.SeriesRef(base + i%numSeries)
			ts := (i / numSeries) * 15000
			tr.Accept(ref, a, ts, h, false)
			tr.Accept(ref, rb, ts+5000, h, false)
			i++
		}
	})
}
