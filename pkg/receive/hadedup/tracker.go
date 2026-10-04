// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package hadedup deduplicates samples of the same series written by multiple HA Prometheus replicas.
// For every series it tracks which replica currently owns it, accepts samples only from the owner and
// fails over to another replica once the owner has been silent for a few of the series' scrape intervals.
// The replica with the lowest label value takes series back as soon as its samples are current again.
package hadedup

import (
	"math"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"go.uber.org/atomic"
)

const (
	// numStripes bounds the time GC holds a stripe lock, as it scans the stripes one at a time.
	numStripes = 256
	noReplica  = math.MaxUint16
	noTs       = math.MinInt64

	// handoverFlag is set in seriesState.intervalMs while ownerLastTs holds the stale handover floor. Learned
	// intervals are capped below it.
	handoverFlag  = 1 << 31
	maxIntervalMs = handoverFlag - 1

	// MaxReplicasLimit is the maximum allowed value of Config.MaxReplicas.
	MaxReplicasLimit = noReplica
	// MaxReplicaValueLength is the maximum length of a replica label value. Replica values are host or pod names,
	// which are at most 63 bytes as DNS labels, so this leaves room for composite values while bounding the replica
	// table to 8 MiB per tenant at MaxReplicasLimit. Series with longer values are written without deduplication.
	MaxReplicaValueLength = 128
)

// Config configures a Tracker.
type Config struct {
	// ReplicaLabel is the name of the label identifying the HA replica. All writers of a tenant must set it: series
	// without it are written without deduplication, even if another replica writes the same series with it.
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
	// StateTTL is how long the state of a series is kept after its newest sample, and how long replica values that
	// are no longer used are kept. Zero means MaxFailoverTimeout + 10m.
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
	if math.IsNaN(c.FailoverIntervals) || math.IsInf(c.FailoverIntervals, 0) || c.FailoverIntervals <= 1 {
		return errors.Errorf("failover intervals must be a finite number greater than 1, got %v", c.FailoverIntervals)
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
	// ownerLastTs is the timestamp of the newest sample accepted from the owner. After a stale handover, while
	// handoverFlag is set, it is the timestamp up to which samples of the new owner are dropped.
	ownerLastTs int64
	// candLastTs is the timestamp of the newest sample seen from cand, noTs if cand is noReplica.
	candLastTs int64
	// intervalMs is the learned sample interval of the owner, 0 if unknown, possibly with handoverFlag set.
	intervalMs uint32
	owner      uint16
	// cand is the non-owner replica with the newest samples that is still sending them, noReplica if none.
	cand uint16
}

func (s *seriesState) interval() uint32 {
	return s.intervalMs &^ handoverFlag
}

type stripe struct {
	mtx    sync.Mutex
	series map[storage.SeriesRef]seriesState
}

type replicaEntry struct {
	value string
	// seen is set on every lookup and consumed by GC to update lastSeen, so lookups don't need to read the clock.
	seen     atomic.Bool
	lastSeen time.Time
}

type metrics struct {
	accepted           prometheus.Counter
	dropped            prometheus.Counter
	failovers          prometheus.Counter
	elections          prometheus.Counter
	handovers          prometheus.Counter
	takeovers          prometheus.Counter
	forgotten          prometheus.Counter
	passthroughNoLabel prometheus.Counter
	passthroughFull    prometheus.Counter
	passthroughTooLong prometheus.Counter
	trackedSeries      prometheus.Gauge
	seriesWithStandby  prometheus.Gauge
	replicas           prometheus.Gauge
	gcRemoved          prometheus.Counter
	gcDuration         prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	samplesTotal := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_receive_ha_dedup_samples_total",
		Help: "Total number of samples from HA replicas processed by deduplication, by outcome.",
	}, []string{"outcome"})
	passthroughTotal := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_receive_ha_dedup_passthrough_total",
		Help: "Total number of series written without HA deduplication, by reason.",
	}, []string{"reason"})
	return &metrics{
		accepted:           samplesTotal.WithLabelValues("accepted"),
		dropped:            samplesTotal.WithLabelValues("dropped"),
		passthroughNoLabel: passthroughTotal.WithLabelValues("no_label"),
		passthroughFull:    passthroughTotal.WithLabelValues("replica_table_full"),
		passthroughTooLong: passthroughTotal.WithLabelValues("replica_value_too_long"),
		failovers: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_failovers_total",
			Help: "Total number of series taken over by another HA replica after the owning replica stopped writing them.",
		}),
		elections: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_elections_total",
			Help: "Total number of series whose owning HA replica was elected by their first tracked sample.",
		}),
		handovers: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_handovers_total",
			Help: "Total number of series handed over to another HA replica after the owning replica wrote a stale marker.",
		}),
		takeovers: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_takeovers_total",
			Help: "Total number of series taken over from a live owning HA replica by the preferred replica, the one with the lowest replica label value.",
		}),
		forgotten: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_forgotten_total",
			Help: "Total number of series states removed because the samples they were based on were not stored.",
		}),
		trackedSeries: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_receive_ha_dedup_tracked_series",
			Help: "Number of series tracked by HA deduplication.",
		}),
		seriesWithStandby: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_receive_ha_dedup_series_with_standby",
			Help: "Number of tracked series with a standby HA replica, as of the last garbage collection.",
		}),
		replicas: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_receive_ha_dedup_replicas",
			Help: "Number of distinct HA replica label values tracked.",
		}),
		gcRemoved: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_ha_dedup_gc_removed_total",
			Help: "Total number of series states removed by HA deduplication garbage collection.",
		}),
		gcDuration: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_receive_ha_dedup_gc_duration_seconds",
			Help: "Duration of the last HA deduplication garbage collection.",
		}),
	}
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
	logger            log.Logger

	replicasMtx sync.RWMutex
	replicaIdx  map[string]uint16
	// replicas is indexed by interned replica index; freed slots are nil and listed in freeReplicas.
	replicas     []*replicaEntry
	freeReplicas []uint16
	// values holds the replica values by interned index, freed slots empty. It is replaced on every change of the
	// replica table, so that decisions can compare replicas without taking replicasMtx.
	values atomic.Pointer[[]string]

	stripes [numStripes]stripe

	now     func() time.Time
	metrics *metrics

	// replicaTableFull and replicaValueTooLong count the series written without deduplication for these reasons
	// since the last GC, which logs them so that the warning is rate limited.
	replicaTableFull    atomic.Int64
	replicaValueTooLong atomic.Int64
}

// NewTracker returns a new Tracker. The config is expected to be valid.
func NewTracker(logger log.Logger, cfg Config, reg prometheus.Registerer) *Tracker {
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
		stateTTL:          stateTTL,
		maxReplicas:       cfg.MaxReplicas,
		logger:            logger,
		replicaIdx:        map[string]uint16{},
		now:               time.Now,
		metrics:           newMetrics(reg),
	}
	t.values.Store(&[]string{})
	return t
}

// ReplicaLabel returns the name of the replica label.
func (t *Tracker) ReplicaLabel() string {
	return t.replicaLabel
}

// LookupReplica returns the interned index of the given replica label value. If the value is not interned, full
// reports whether the replica table has no room for it. A returned index stays valid for at least the state TTL, as
// the lookup keeps GC from freeing it for that long.
func (t *Tracker) LookupReplica(value string) (r uint16, ok, full bool) {
	t.replicasMtx.RLock()
	defer t.replicasMtx.RUnlock()
	r, ok = t.replicaIdx[value]
	if !ok {
		return 0, false, len(t.replicaIdx) >= t.maxReplicas
	}
	if rep := t.replicas[r]; !rep.seen.Load() {
		rep.seen.Store(true)
	}
	return r, true, false
}

// Replica returns the interned index of the given replica label value, interning it if needed. It returns false if
// the replica table is full, in which case the series must be written without deduplication. Values must be interned
// only for samples that take part in a decision, so that series without valid samples can't fill the table.
func (t *Tracker) Replica(value string) (uint16, bool) {
	if r, ok, full := t.LookupReplica(value); ok || full {
		return r, ok
	}

	t.replicasMtx.Lock()
	r, ok := t.replicaIdx[value]
	if ok {
		t.replicas[r].seen.Store(true)
		t.replicasMtx.Unlock()
		return r, true
	}
	if len(t.replicaIdx) >= t.maxReplicas {
		t.replicasMtx.Unlock()
		return 0, false
	}

	// The value references request memory, detach it before keeping it.
	rep := &replicaEntry{value: strings.Clone(value), lastSeen: t.now()}
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
	t.storeValues()
	n := len(t.replicaIdx)
	t.metrics.replicas.Set(float64(n))
	t.replicasMtx.Unlock()

	level.Info(t.logger).Log("msg", "tracking new HA replica", "replica", rep.value, "replicas", n)
	return r, true
}

// Counts are the outcomes of the deduplication decisions taken for a write request, see Record.
type Counts struct {
	// Accepted and Dropped count the samples of deduplicated series.
	Accepted int
	Dropped  int
	// Elections, Failovers, Handovers and Takeovers count the series whose owner was elected or changed, see Change.
	Elections int
	Failovers int
	Handovers int
	Takeovers int
	// Forgotten counts the series removed by Forget.
	Forgotten int
	// NoLabel, ReplicaTableFull and ReplicaValueTooLong count the series written without deduplication, by reason.
	NoLabel             int
	ReplicaTableFull    int
	ReplicaValueTooLong int
}

// Record adds the outcomes of a write request to the metrics. Writers count decisions per request instead of the
// tracker counting them per sample, so that concurrent writers of a tenant don't contend on the shared counters.
func (t *Tracker) Record(c Counts) {
	addCount(t.metrics.accepted, c.Accepted)
	addCount(t.metrics.dropped, c.Dropped)
	addCount(t.metrics.failovers, c.Failovers)
	addCount(t.metrics.elections, c.Elections)
	addCount(t.metrics.handovers, c.Handovers)
	addCount(t.metrics.takeovers, c.Takeovers)
	addCount(t.metrics.forgotten, c.Forgotten)
	addCount(t.metrics.passthroughNoLabel, c.NoLabel)
	addCount(t.metrics.passthroughFull, c.ReplicaTableFull)
	addCount(t.metrics.passthroughTooLong, c.ReplicaValueTooLong)
	if c.ReplicaTableFull > 0 {
		t.replicaTableFull.Add(int64(c.ReplicaTableFull))
	}
	if c.ReplicaValueTooLong > 0 {
		t.replicaValueTooLong.Add(int64(c.ReplicaValueTooLong))
	}
}

func addCount(c prometheus.Counter, n int) {
	if n > 0 {
		c.Add(float64(n))
	}
}

// Change is the change of a series' owner by a decision.
type Change uint8

const (
	// NoChange means that the owner was kept.
	NoChange Change = iota
	// Elected means that the series state was created with the sample's replica as owner.
	Elected
	// Failover means that the sample's replica took the series over from an owner that timed out.
	Failover
	// Handover means that the owner's stale marker handed the series over to the standby replica.
	Handover
	// Takeover means that the preferred replica took the series over from a live owner.
	Takeover
)

// Horizon bounds the sample timestamps that ownership decisions take into account. Writers take it once per request,
// so that deciding on samples doesn't read the clock.
type Horizon struct {
	now int64
	max int64
}

// Horizon returns the horizon of decisions taken now. Timestamps of an owner's samples are recorded up to the max
// failover timeout ahead of the current time, for clock skew between the replicas and Receive.
func (t *Tracker) Horizon() Horizon {
	now := t.now().UnixMilli()
	return Horizon{now: now, max: now + t.maxTimeoutMs}
}

// Accept decides whether the sample at ts written by replica to the series with the given head reference
// should be appended, and updates the series state accordingly. A change of the owner is also reported for a
// dropped sample: the caller must Forget the series if the samples the decision is based on are not stored.
func (t *Tracker) Accept(ref storage.SeriesRef, replica uint16, ts int64, h Horizon, stale bool) (accept bool, change Change) {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	st, exists := s.series[ref]
	accept, change = t.decide(&st, exists, replica, ts, h, stale)
	if s.series == nil {
		s.series = map[storage.SeriesRef]seriesState{}
	}
	s.series[ref] = st
	s.mtx.Unlock()

	if !exists {
		t.metrics.trackedSeries.Inc()
	}
	return accept, change
}

// Forget removes the state of a series, so that its next sample elects the owner again. It returns true if the
// series was tracked.
func (t *Tracker) Forget(ref storage.SeriesRef) bool {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	_, exists := s.series[ref]
	delete(s.series, ref)
	s.mtx.Unlock()

	if exists {
		t.metrics.trackedSeries.Dec()
	}
	return exists
}

// Init records replica as the owner of a series created by appending its sample at ts, unless another
// writer created state for the series in the meantime. In that case both replicas' first samples may have
// been appended. It returns true if it created the state.
func (t *Tracker) Init(ref storage.SeriesRef, replica uint16, ts int64, h Horizon) bool {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	_, exists := s.series[ref]
	if !exists {
		if s.series == nil {
			s.series = map[storage.SeriesRef]seriesState{}
		}
		s.series[ref] = newOwnerState(replica, ts, 0, h)
	}
	s.mtx.Unlock()

	if !exists {
		t.metrics.trackedSeries.Inc()
	}
	return !exists
}

// IsOwner returns true if replica owns the series or the series is not tracked.
func (t *Tracker) IsOwner(ref storage.SeriesRef, replica uint16) bool {
	s := &t.stripes[uint64(ref)%numStripes]
	s.mtx.Lock()
	st, exists := s.series[ref]
	s.mtx.Unlock()
	return !exists || st.owner == replica
}

// decide implements the per-sample state machine. Decisions compare sample timestamps rather than arrival times, so
// lag that all replicas share doesn't cause failovers, while an owner lagging behind another replica by more than the
// failover timeout loses its series. Timestamps are only trusted up to the horizon: no stored timestamp is beyond
// h.max, and the part of a timestamp beyond the current time isn't taken as evidence that the owner went silent, so
// that samples from the future can't take series over from a live owner.
func (t *Tracker) decide(s *seriesState, exists bool, r uint16, ts int64, h Horizon, stale bool) (accept bool, change Change) {
	if !exists {
		*s = newOwnerState(r, ts, 0, h)
		return true, Elected
	}

	if r == s.owner {
		if s.intervalMs&handoverFlag != 0 {
			if ts <= s.ownerLastTs {
				return false, NoChange
			}
			// The distance to the floor is the phase offset between the replicas, not an interval of the new owner.
			s.intervalMs &^= handoverFlag
			s.ownerLastTs = min(ts, h.max)
		}
		if stale && s.cand != noReplica && !later(ts, s.candLastTs, t.timeout(s.interval())) {
			// The series vanished only on the owner while another replica still produces it: hand the series
			// over and drop the stale marker. The new owner may lag behind the old one, so its samples up to the
			// newest one already appended are dropped instead of being appended out of order. Resetting cand makes
			// the new owner's own stale marker be accepted if the series vanishes there too, instead of handing it
			// back and forth.
			s.owner, s.ownerLastTs = s.cand, max(s.ownerLastTs, s.candLastTs)
			s.cand, s.candLastTs = noReplica, noTs
			s.intervalMs |= handoverFlag
			return false, Handover
		}
		switch {
		case ts > h.max:
			// The owner's own data is appended as without deduplication, but recording its timestamp would keep
			// other replicas from taking over until then. The delta to it isn't an interval either.
			s.ownerLastTs = max(s.ownerLastTs, h.max)
		case ts > s.ownerLastTs:
			s.intervalMs = learnInterval(s.intervalMs, ts-s.ownerLastTs)
			s.ownerLastTs = ts
		}
		return true, NoChange
	}

	if later(min(ts, h.now), s.ownerLastTs, t.timeout(s.interval())) {
		*s = newOwnerState(r, ts, s.interval(), h)
		return true, Failover
	}

	if stale {
		// A replica that sent a stale marker no longer produces the series, so it can't take it over.
		if s.cand == r {
			s.cand, s.candLastTs = noReplica, noTs
		}
		return false, NoChange
	}
	// The preferred replica takes over once it is current, i.e. not behind any sample already appended. A lagging
	// preferred replica stays standby instead, as taking over would drop the owner's newer samples. A sample from the
	// future doesn't take over either: appending it would make the owner's later samples out of order.
	if ts > s.ownerLastTs && ts <= h.now && t.prefers(r, s.owner) {
		*s = newOwnerState(r, ts, s.interval(), h)
		return true, Takeover
	}
	// Replacing cand by arrival order would let a lagging replica evict a live one, and the owner's stale marker
	// would then be accepted instead of handing the series over. A timestamp from the future counts as the current
	// time: the handover floor is derived from it, so it would otherwise keep the other replicas out until then.
	if ts := min(ts, h.now); ts > s.candLastTs {
		s.cand, s.candLastTs = r, ts
	}
	return false, NoChange
}

// prefers reports whether replica r has a lower label value than replica o. The ingestors holding copies of a series
// elect its owner independently, e.g. one of them after a restart, and the copies stay with different owners while
// both replicas write the series. Taking series over by a fixed preference makes them converge within one sample.
func (t *Tracker) prefers(r, o uint16) bool {
	values := *t.values.Load()
	return int(max(r, o)) < len(values) && values[r] < values[o]
}

// storeValues publishes the current replica values for prefers. replicasMtx must be held for writing.
func (t *Tracker) storeValues() {
	values := make([]string, len(t.replicas))
	for i, rep := range t.replicas {
		if rep != nil {
			values[i] = rep.value
		}
	}
	t.values.Store(&values)
}

// newOwnerState returns the state of a series whose owner became r with its sample at ts. A new owner's timestamp is
// recorded at most at the current time, so that a single sample from the future can't hold the series beyond the
// failover timeout. A live owner whose clock is ahead moves it forward with its next sample.
func newOwnerState(r uint16, ts int64, intervalMs uint32, h Horizon) seriesState {
	return seriesState{owner: r, ownerLastTs: min(ts, h.now), intervalMs: intervalMs, cand: noReplica, candLastTs: noTs}
}

// later reports whether ts is more than d after last. Written without last+d, which overflows for timestamps near
// the int64 limits that a writer may send.
func later(ts, last, d int64) bool {
	return ts > last && uint64(ts-last) > uint64(d)
}

func (t *Tracker) timeout(intervalMs uint32) int64 {
	if intervalMs == 0 {
		return t.defaultTimeoutMs
	}
	// Clamped before the conversion, which is undefined for values beyond the int64 range.
	return int64(min(max(t.failoverIntervals*float64(intervalMs), float64(t.minTimeoutMs)), float64(t.maxTimeoutMs)))
}

// learnInterval tracks the smallest recent delta between consecutive owner samples: smaller deltas are adopted
// immediately, larger ones move the interval by 1/8 of the difference, at most up to double the current interval.
// Occasional gaps are thus undone by the next regular sample, while a real interval change is learned over a few
// dozen samples.
func learnInterval(cur uint32, delta int64) uint32 {
	if delta <= 0 || delta > maxIntervalMs {
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
// based on the current time. It also removes state with timestamps further in the future than any decision can
// store, which is left over from before a backward step of the clock. It returns the number of removed series.
func (t *Tracker) GC() int {
	start := time.Now()
	defer func() { t.metrics.gcDuration.Set(time.Since(start).Seconds()) }()

	now := t.now()
	cutoff := now.Add(-t.stateTTL).UnixMilli()
	futureCutoff := now.Add(t.stateTTL).UnixMilli() + t.maxTimeoutMs

	t.replicasMtx.RLock()
	referenced := make([]bool, len(t.replicas))
	t.replicasMtx.RUnlock()

	var removed, withStandby int
	for i := range t.stripes {
		s := &t.stripes[i]
		s.mtx.Lock()
		for ref, st := range s.series {
			if newest := max(st.ownerLastTs, st.candLastTs); newest < cutoff || newest > futureCutoff {
				delete(s.series, ref)
				removed++
				continue
			}
			markReferenced(referenced, st.owner)
			if st.cand != noReplica {
				markReferenced(referenced, st.cand)
				withStandby++
			}
		}
		s.mtx.Unlock()
	}

	t.metrics.trackedSeries.Sub(float64(removed))
	t.metrics.gcRemoved.Add(float64(removed))
	t.metrics.seriesWithStandby.Set(float64(withStandby))
	t.logPassthrough()

	t.replicasMtx.Lock()
	defer t.replicasMtx.Unlock()
	var freed bool
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
		freed = true
	}
	if freed {
		t.storeValues()
	}
	t.metrics.replicas.Set(float64(len(t.replicaIdx)))
	return removed
}

func (t *Tracker) logPassthrough() {
	if n := t.replicaTableFull.Swap(0); n > 0 {
		level.Warn(t.logger).Log("msg", "HA replica table is full, series of further replicas are written without deduplication", "series", n, "maxReplicas", t.maxReplicas)
	}
	if n := t.replicaValueTooLong.Swap(0); n > 0 {
		level.Warn(t.logger).Log("msg", "HA replica label values are too long, their series are written without deduplication", "series", n, "maxLength", MaxReplicaValueLength)
	}
}

func markReferenced(referenced []bool, r uint16) {
	if int(r) < len(referenced) {
		referenced[r] = true
	}
}
