// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package hadedup deduplicates samples of the same series written by multiple HA Prometheus replicas.
// For every series it tracks which replica currently owns it, accepts samples only from the owner and
// fails over to another replica once the owner has been silent for a few of the series' scrape intervals.
package hadedup

import (
	"math"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/storage"
	"go.uber.org/atomic"
)

const (
	numStripes = 64
	noReplica  = math.MaxUint16

	// MaxReplicasLimit is the maximum allowed value of Config.MaxReplicas.
	MaxReplicasLimit = noReplica

	failoverReasonTimeout       = "timeout"
	failoverReasonStaleHandover = "stale_handover"

	PassthroughReasonNoLabel          = "no_label"
	PassthroughReasonReplicaTableFull = "replica_table_full"
)

// Config configures a Tracker.
type Config struct {
	// ReplicaLabel is the name of the label identifying the HA replica.
	ReplicaLabel string
	// FailoverIntervals is the number of learned scrape intervals of owner silence after which another replica takes over.
	FailoverIntervals float64
	// MinFailoverTimeout and MaxFailoverTimeout clamp the failover timeout computed from the learned interval.
	MinFailoverTimeout time.Duration
	MaxFailoverTimeout time.Duration
	// DefaultFailoverTimeout is used while no interval has been learned for a series yet.
	DefaultFailoverTimeout time.Duration
	// MaxReplicas caps the number of distinct replica label values tracked per tenant.
	MaxReplicas int
	// StateTTL is how long series state is kept after its newest sample, relative to the newest sample seen by the tracker.
	// Zero means MaxFailoverTimeout + 10m.
	StateTTL time.Duration
}

// Validate returns an error if the configuration is invalid.
func (c Config) Validate() error {
	if c.ReplicaLabel == "" {
		return errors.New("replica label must be set")
	}
	if c.FailoverIntervals <= 1 {
		return errors.Errorf("failover intervals must be greater than 1, got %v", c.FailoverIntervals)
	}
	if c.MinFailoverTimeout <= 0 || c.MaxFailoverTimeout <= 0 || c.DefaultFailoverTimeout <= 0 {
		return errors.New("failover timeouts must be positive")
	}
	if c.MinFailoverTimeout > c.MaxFailoverTimeout {
		return errors.Errorf("min failover timeout %v must not be greater than max failover timeout %v", c.MinFailoverTimeout, c.MaxFailoverTimeout)
	}
	if c.MaxReplicas <= 0 || c.MaxReplicas > MaxReplicasLimit {
		return errors.Errorf("max replicas must be between 1 and %d, got %d", MaxReplicasLimit, c.MaxReplicas)
	}
	if c.StateTTL < 0 {
		return errors.New("state TTL must not be negative")
	}
	return nil
}

type seriesState struct {
	// ownerLastTs is the timestamp of the newest sample accepted from the owner.
	ownerLastTs int64
	// candLastTs is the timestamp of the newest sample seen from cand.
	candLastTs int64
	// intervalMs is the learned sample interval of the owner, 0 if unknown.
	intervalMs uint32
	owner      uint16
	// cand is the most recent non-owner replica that is still sending samples, noReplica if none.
	cand uint16
}

type stripe struct {
	mtx    sync.Mutex
	series map[storage.SeriesRef]seriesState
}

type metrics struct {
	samplesTotal     *prometheus.CounterVec
	failoversTotal   *prometheus.CounterVec
	trackedSeries    prometheus.Gauge
	replicas         prometheus.Gauge
	passthroughTotal *prometheus.CounterVec
	gcRemoved        prometheus.Counter

	accepted           prometheus.Counter
	dropped            prometheus.Counter
	failoversTimeout   prometheus.Counter
	failoversStale     prometheus.Counter
	passthroughNoLabel prometheus.Counter
	passthroughFull    prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		samplesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_samples_total",
			Help: "Total number of samples from HA replicas processed by deduplication, by outcome.",
		}, []string{"outcome"}),
		failoversTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_failovers_total",
			Help: "Total number of series ownership changes between HA replicas, by reason.",
		}, []string{"reason"}),
		trackedSeries: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_receive_ha_dedup_tracked_series",
			Help: "Number of series tracked by HA deduplication.",
		}),
		replicas: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_receive_ha_dedup_replicas",
			Help: "Number of distinct HA replica label values seen.",
		}),
		passthroughTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_passthrough_total",
			Help: "Total number of series written without HA deduplication, by reason.",
		}, []string{"reason"}),
		gcRemoved: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_gc_removed_total",
			Help: "Total number of series states removed by HA deduplication garbage collection.",
		}),
	}
	m.accepted = m.samplesTotal.WithLabelValues("accepted")
	m.dropped = m.samplesTotal.WithLabelValues("dropped")
	m.failoversTimeout = m.failoversTotal.WithLabelValues(failoverReasonTimeout)
	m.failoversStale = m.failoversTotal.WithLabelValues(failoverReasonStaleHandover)
	m.passthroughNoLabel = m.passthroughTotal.WithLabelValues(PassthroughReasonNoLabel)
	m.passthroughFull = m.passthroughTotal.WithLabelValues(PassthroughReasonReplicaTableFull)
	return m
}

// Tracker keeps per-series ownership state of HA replicas for a single tenant. It is safe for concurrent use.
type Tracker struct {
	replicaLabel      string
	failoverIntervals float64
	minTimeoutMs      int64
	maxTimeoutMs      int64
	defaultTimeoutMs  int64
	stateTTLMs        int64
	maxReplicas       int

	replicasMtx sync.RWMutex
	replicas    map[string]uint16

	stripes [numStripes]stripe

	// maxTs is the newest sample timestamp seen, used as the reference point for GC.
	maxTs atomic.Int64

	metrics *metrics
}

// NewTracker returns a new Tracker. The config is expected to be valid.
func NewTracker(cfg Config, reg prometheus.Registerer) *Tracker {
	stateTTL := cfg.StateTTL
	if stateTTL == 0 {
		stateTTL = cfg.MaxFailoverTimeout + 10*time.Minute
	}
	t := &Tracker{
		replicaLabel:      cfg.ReplicaLabel,
		failoverIntervals: cfg.FailoverIntervals,
		minTimeoutMs:      cfg.MinFailoverTimeout.Milliseconds(),
		maxTimeoutMs:      cfg.MaxFailoverTimeout.Milliseconds(),
		defaultTimeoutMs:  cfg.DefaultFailoverTimeout.Milliseconds(),
		stateTTLMs:        stateTTL.Milliseconds(),
		maxReplicas:       cfg.MaxReplicas,
		replicas:          map[string]uint16{},
		metrics:           newMetrics(reg),
	}
	t.maxTs.Store(math.MinInt64)
	return t
}

// ReplicaLabel returns the name of the replica label.
func (t *Tracker) ReplicaLabel() string {
	return t.replicaLabel
}

// Replica returns the interned index of the given replica label value. It returns false if the replica table is full,
// in which case the series must be written without deduplication.
func (t *Tracker) Replica(value string) (uint16, bool) {
	t.replicasMtx.RLock()
	r, ok := t.replicas[value]
	t.replicasMtx.RUnlock()
	if ok {
		return r, true
	}

	t.replicasMtx.Lock()
	defer t.replicasMtx.Unlock()
	if r, ok := t.replicas[value]; ok {
		return r, true
	}
	if len(t.replicas) >= t.maxReplicas {
		return 0, false
	}
	r = uint16(len(t.replicas))
	// The value references request memory, detach it before keeping it.
	t.replicas[strings.Clone(value)] = r
	t.metrics.replicas.Set(float64(len(t.replicas)))
	return r, true
}

// Passthrough records a series written without deduplication.
func (t *Tracker) Passthrough(reason string) {
	switch reason {
	case PassthroughReasonNoLabel:
		t.metrics.passthroughNoLabel.Inc()
	case PassthroughReasonReplicaTableFull:
		t.metrics.passthroughFull.Inc()
	}
}

// Accept decides whether the sample at ts written by replica to the series with the given head reference
// should be appended, and updates the series state accordingly.
func (t *Tracker) Accept(ref storage.SeriesRef, replica uint16, ts int64, stale bool) bool {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	st, exists := s.series[ref]
	accept, reason := t.decide(&st, exists, replica, ts, stale)
	if s.series == nil {
		s.series = map[storage.SeriesRef]seriesState{}
	}
	s.series[ref] = st
	s.mtx.Unlock()

	if !exists {
		t.metrics.trackedSeries.Inc()
	}
	t.observe(ts, accept, reason)
	return accept
}

// Init records replica as the owner of a series created by appending its sample at ts, unless another
// writer created state for the series in the meantime.
func (t *Tracker) Init(ref storage.SeriesRef, replica uint16, ts int64) {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	_, exists := s.series[ref]
	if !exists {
		if s.series == nil {
			s.series = map[storage.SeriesRef]seriesState{}
		}
		s.series[ref] = seriesState{owner: replica, ownerLastTs: ts, cand: noReplica}
	}
	s.mtx.Unlock()

	if !exists {
		t.metrics.trackedSeries.Inc()
	}
	t.observe(ts, true, "")
}

// IsOwner returns true if replica owns the series or the series is not tracked.
func (t *Tracker) IsOwner(ref storage.SeriesRef, replica uint16) bool {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	st, exists := s.series[ref]
	s.mtx.Unlock()
	return !exists || st.owner == replica
}

func (t *Tracker) observe(ts int64, accept bool, failoverReason string) {
	for {
		cur := t.maxTs.Load()
		if ts <= cur || t.maxTs.CompareAndSwap(cur, ts) {
			break
		}
	}
	if accept {
		t.metrics.accepted.Inc()
	} else {
		t.metrics.dropped.Inc()
	}
	switch failoverReason {
	case failoverReasonTimeout:
		t.metrics.failoversTimeout.Inc()
	case failoverReasonStaleHandover:
		t.metrics.failoversStale.Inc()
	}
}

// decide implements the per-sample state machine. All times are sample timestamps, never wall clock: this makes
// decisions independent of remote-write delivery lag, so a lagging owner loses ownership to the most current replica.
func (t *Tracker) decide(s *seriesState, exists bool, r uint16, ts int64, stale bool) (bool, string) {
	if !exists {
		*s = seriesState{owner: r, ownerLastTs: ts, cand: noReplica}
		return true, ""
	}

	if r == s.owner {
		if ts > s.ownerLastTs {
			s.intervalMs = learnInterval(s.intervalMs, ts-s.ownerLastTs)
			s.ownerLastTs = ts
		}
		if stale && s.cand != noReplica && s.candLastTs >= ts-t.timeout(s.intervalMs) {
			// The series vanished only on the owner while another replica still produces it: hand the series
			// over and drop the stale marker. Resetting cand makes the new owner's own stale marker be accepted
			// if the series vanishes there too, instead of handing it back and forth.
			s.owner, s.ownerLastTs = s.cand, s.candLastTs
			s.cand, s.candLastTs = noReplica, 0
			return false, failoverReasonStaleHandover
		}
		return true, ""
	}

	if ts > s.ownerLastTs+t.timeout(s.intervalMs) {
		*s = seriesState{owner: r, ownerLastTs: ts, intervalMs: s.intervalMs, cand: noReplica}
		return true, failoverReasonTimeout
	}

	if stale {
		// A replica that sent a stale marker no longer produces the series, so it can't take it over.
		if s.cand == r {
			s.cand, s.candLastTs = noReplica, 0
		}
		return false, ""
	}
	if s.cand != r {
		s.cand, s.candLastTs = r, ts
	} else if ts > s.candLastTs {
		s.candLastTs = ts
	}
	return false, ""
}

func (t *Tracker) timeout(intervalMs uint32) int64 {
	if intervalMs == 0 {
		return t.defaultTimeoutMs
	}
	return min(max(int64(t.failoverIntervals*float64(intervalMs)), t.minTimeoutMs), t.maxTimeoutMs)
}

// learnInterval keeps the smallest plausible delta between consecutive owner samples. Deltas of at least twice the
// current interval are treated as gaps (missed scrapes, restarts) and ignored.
func learnInterval(cur uint32, delta int64) uint32 {
	if delta <= 0 || delta > math.MaxUint32 {
		return cur
	}
	if cur == 0 || delta < 2*int64(cur) {
		return uint32(delta)
	}
	return cur
}

// GC removes the state of series whose newest sample is older than the state TTL, relative to the newest sample
// seen by the tracker. It returns the number of removed series.
func (t *Tracker) GC() int {
	maxTs := t.maxTs.Load()
	if maxTs == math.MinInt64 {
		return 0
	}
	cutoff := maxTs - t.stateTTLMs

	var removed int
	for i := range t.stripes {
		s := &t.stripes[i]
		s.mtx.Lock()
		for ref, st := range s.series {
			if max(st.ownerLastTs, st.candLastTs) < cutoff {
				delete(s.series, ref)
				removed++
			}
		}
		s.mtx.Unlock()
	}

	t.metrics.trackedSeries.Sub(float64(removed))
	t.metrics.gcRemoved.Add(float64(removed))
	return removed
}
