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
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"go.uber.org/atomic"
)

const (
	numStripes = 64
	noReplica  = math.MaxUint16
	noTs       = math.MinInt64

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
	// StateTTL is how long state is kept for series whose newest sample is older than the current time, and for
	// replica values that are no longer used. Zero means MaxFailoverTimeout + 10m.
	StateTTL time.Duration
}

// Validate returns an error if the configuration is invalid.
func (c Config) Validate() error {
	if c.ReplicaLabel == "" {
		return errors.New("replica label must be set")
	}
	if strings.HasPrefix(c.ReplicaLabel, model.ReservedLabelPrefix) {
		return errors.Errorf("replica label %s must not use the reserved prefix %s", c.ReplicaLabel, model.ReservedLabelPrefix)
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
	if c.DefaultFailoverTimeout < c.MinFailoverTimeout || c.DefaultFailoverTimeout > c.MaxFailoverTimeout {
		return errors.Errorf("default failover timeout %v must be between min failover timeout %v and max failover timeout %v", c.DefaultFailoverTimeout, c.MinFailoverTimeout, c.MaxFailoverTimeout)
	}
	if c.MaxReplicas <= 0 || c.MaxReplicas > MaxReplicasLimit {
		return errors.Errorf("max replicas must be between 1 and %d, got %d", MaxReplicasLimit, c.MaxReplicas)
	}
	if c.StateTTL < 0 {
		return errors.New("state TTL must not be negative")
	}
	if c.StateTTL != 0 && c.StateTTL < c.MaxFailoverTimeout {
		return errors.Errorf("state TTL %v must not be less than max failover timeout %v", c.StateTTL, c.MaxFailoverTimeout)
	}
	return nil
}

type seriesState struct {
	// ownerLastTs is the timestamp of the newest sample accepted from the owner.
	ownerLastTs int64
	// candLastTs is the timestamp of the newest sample seen from cand. While cand is noReplica it holds the
	// timestamp up to which samples of the owner are dropped after a stale handover, or noTs.
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

type replica struct {
	value string
	// seen is set on every lookup and consumed by GC to update lastSeen, so lookups don't need to read the clock.
	seen     atomic.Bool
	lastSeen time.Time
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
			Help: "Number of distinct HA replica label values tracked.",
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
	stateTTL          time.Duration
	maxReplicas       int

	replicasMtx sync.RWMutex
	replicaIdx  map[string]uint16
	// replicas is indexed by interned replica index; freed slots are nil and listed in freeReplicas.
	replicas     []*replica
	freeReplicas []uint16

	stripes [numStripes]stripe

	now     func() time.Time
	metrics *metrics
}

// NewTracker returns a new Tracker. The config is expected to be valid.
func NewTracker(cfg Config, reg prometheus.Registerer) *Tracker {
	stateTTL := cfg.StateTTL
	if stateTTL == 0 {
		stateTTL = cfg.MaxFailoverTimeout + 10*time.Minute
	}
	return &Tracker{
		replicaLabel:      cfg.ReplicaLabel,
		failoverIntervals: cfg.FailoverIntervals,
		minTimeoutMs:      cfg.MinFailoverTimeout.Milliseconds(),
		maxTimeoutMs:      cfg.MaxFailoverTimeout.Milliseconds(),
		defaultTimeoutMs:  cfg.DefaultFailoverTimeout.Milliseconds(),
		stateTTL:          stateTTL,
		maxReplicas:       cfg.MaxReplicas,
		replicaIdx:        map[string]uint16{},
		now:               time.Now,
		metrics:           newMetrics(reg),
	}
}

// ReplicaLabel returns the name of the replica label.
func (t *Tracker) ReplicaLabel() string {
	return t.replicaLabel
}

// Replica returns the interned index of the given replica label value. It returns false if the replica table is full,
// in which case the series must be written without deduplication.
func (t *Tracker) Replica(value string) (uint16, bool) {
	t.replicasMtx.RLock()
	r, ok := t.replicaIdx[value]
	if ok {
		if rep := t.replicas[r]; !rep.seen.Load() {
			rep.seen.Store(true)
		}
	}
	t.replicasMtx.RUnlock()
	if ok {
		return r, true
	}

	t.replicasMtx.Lock()
	defer t.replicasMtx.Unlock()
	if r, ok := t.replicaIdx[value]; ok {
		t.replicas[r].seen.Store(true)
		return r, true
	}
	if len(t.replicaIdx) >= t.maxReplicas {
		return 0, false
	}

	// The value references request memory, detach it before keeping it.
	rep := &replica{value: strings.Clone(value), lastSeen: t.now()}
	rep.seen.Store(true)
	if n := len(t.freeReplicas); n > 0 {
		r = t.freeReplicas[n-1]
		t.freeReplicas = t.freeReplicas[:n-1]
		t.replicas[r] = rep
	} else {
		r = uint16(len(t.replicas))
		t.replicas = append(t.replicas, rep)
	}
	t.replicaIdx[rep.value] = r
	t.metrics.replicas.Set(float64(len(t.replicaIdx)))
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
	t.observe(accept, reason)
	return accept
}

// Init records replica as the owner of a series created by appending its sample at ts, unless another
// writer created state for the series in the meantime. In that case both replicas' first samples may have
// been appended.
func (t *Tracker) Init(ref storage.SeriesRef, replica uint16, ts int64) {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	_, exists := s.series[ref]
	if !exists {
		if s.series == nil {
			s.series = map[storage.SeriesRef]seriesState{}
		}
		s.series[ref] = seriesState{owner: replica, ownerLastTs: ts, cand: noReplica, candLastTs: noTs}
	}
	s.mtx.Unlock()

	if !exists {
		t.metrics.trackedSeries.Inc()
	}
	t.observe(true, "")
}

// IsOwner returns true if replica owns the series or the series is not tracked.
func (t *Tracker) IsOwner(ref storage.SeriesRef, replica uint16) bool {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	st, exists := s.series[ref]
	s.mtx.Unlock()
	return !exists || st.owner == replica
}

func (t *Tracker) observe(accept bool, failoverReason string) {
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
		*s = seriesState{owner: r, ownerLastTs: ts, cand: noReplica, candLastTs: noTs}
		return true, ""
	}

	if r == s.owner {
		if s.cand == noReplica && ts <= s.candLastTs {
			return false, ""
		}
		if stale && s.cand != noReplica && s.candLastTs >= ts-t.timeout(s.intervalMs) {
			// The series vanished only on the owner while another replica still produces it: hand the series
			// over and drop the stale marker. The new owner may lag behind the old one, so its samples up to the
			// newest one already appended are dropped instead of being appended out of order. Resetting cand makes
			// the new owner's own stale marker be accepted if the series vanishes there too, instead of handing it
			// back and forth.
			last := max(s.ownerLastTs, s.candLastTs)
			s.owner, s.ownerLastTs = s.cand, last
			s.cand, s.candLastTs = noReplica, last
			return false, failoverReasonStaleHandover
		}
		if ts > s.ownerLastTs {
			s.intervalMs = learnInterval(s.intervalMs, ts-s.ownerLastTs)
			s.ownerLastTs = ts
		}
		return true, ""
	}

	if ts > s.ownerLastTs+t.timeout(s.intervalMs) {
		*s = seriesState{owner: r, ownerLastTs: ts, intervalMs: s.intervalMs, cand: noReplica, candLastTs: noTs}
		return true, failoverReasonTimeout
	}

	if stale {
		// A replica that sent a stale marker no longer produces the series, so it can't take it over.
		if s.cand == r {
			s.cand, s.candLastTs = noReplica, noTs
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

// learnInterval tracks the smallest recent delta between consecutive owner samples: smaller deltas are adopted
// immediately, larger ones move the interval by 1/8 of the difference, at most up to double the current interval.
// Occasional gaps are thus undone by the next regular sample, while a real interval change is learned over a few
// dozen samples.
func learnInterval(cur uint32, delta int64) uint32 {
	if delta <= 0 || delta > math.MaxUint32 {
		return cur
	}
	if cur == 0 || delta <= int64(cur) {
		return uint32(delta)
	}
	target := min(delta, 2*int64(cur))
	return cur + uint32(max((target-int64(cur))/8, 1))
}

// GC removes the state of series whose newest sample is older than the state TTL, and frees replica values that
// were not used for longer than the state TTL and are not referenced by any series. Unlike failover decisions, GC is
// based on the current time, so that samples with timestamps in the future can't keep other state from being removed.
// It returns the number of removed series.
func (t *Tracker) GC() int {
	now := t.now()
	cutoff := now.Add(-t.stateTTL).UnixMilli()

	t.replicasMtx.RLock()
	referenced := make([]bool, len(t.replicas))
	t.replicasMtx.RUnlock()

	var removed int
	for i := range t.stripes {
		s := &t.stripes[i]
		s.mtx.Lock()
		for ref, st := range s.series {
			if max(st.ownerLastTs, st.candLastTs) < cutoff {
				delete(s.series, ref)
				removed++
				continue
			}
			markReferenced(referenced, st.owner)
			if st.cand != noReplica {
				markReferenced(referenced, st.cand)
			}
		}
		s.mtx.Unlock()
	}

	t.metrics.trackedSeries.Sub(float64(removed))
	t.metrics.gcRemoved.Add(float64(removed))

	t.replicasMtx.Lock()
	defer t.replicasMtx.Unlock()
	for i, rep := range t.replicas {
		if rep == nil {
			continue
		}
		// Replicas interned after the series were scanned are always seen, so they are never freed here.
		if rep.seen.Swap(false) {
			rep.lastSeen = now
			continue
		}
		if (i < len(referenced) && referenced[i]) || now.Sub(rep.lastSeen) <= t.stateTTL {
			continue
		}
		delete(t.replicaIdx, rep.value)
		t.replicas[i] = nil
		t.freeReplicas = append(t.freeReplicas, uint16(i))
	}
	t.metrics.replicas.Set(float64(len(t.replicaIdx)))
	return removed
}

func markReferenced(referenced []bool, r uint16) {
	if int(r) < len(referenced) {
		referenced[r] = true
	}
}
