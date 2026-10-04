// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"slices"
	"strings"

	"github.com/colega/zeropool"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/receive/hadedup"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// haDedupTenantStorage is implemented by tenant storages that deduplicate samples of HA Prometheus replicas.
type haDedupTenantStorage interface {
	// TenantHADedupTracker returns the tracker of the given tenant, or nil if deduplication is disabled.
	TenantHADedupTracker(tenantID string) (*hadedup.Tracker, error)
}

// haDedupChangedPool reuses haDedupWriter.changed, which has an entry per series of a request with accepted samples.
var haDedupChangedPool zeropool.Pool[[]storage.SeriesRef]

// haDedupWriter applies HA replica deduplication to the series of a single write request.
// It is shared by Writer and CapNProtoWriter so that both paths take the same decisions.
type haDedupWriter struct {
	tracker *hadedup.Tracker
	horizon hadedup.Horizon
	scratch []labelpb.ZLabel
	// changed lists the series whose state is based on samples of the request, see forget.
	changed []storage.SeriesRef
	counts  hadedup.Counts
	// tableFilled is set if the replica table filled up while the request was written, see intern.
	tableFilled bool

	// The series of a request normally come from a single Prometheus, so the lookup of the last replica value is
	// reused while it repeats. The value references request memory without being copied: the request's label
	// strings stay unmodified until the request is written, prepare with inPlace only moves label headers.
	lastReplicaValue  string
	lastReplica       uint16
	lastReplicaStatus replicaStatus
}

type replicaStatus uint8

const (
	replicaNotLookedUp replicaStatus = iota
	replicaInterned
	// replicaUnknown is a value that is not interned yet, see haDedupWriter.intern.
	replicaUnknown
	replicaTableFull
)

func newHADedupWriter(s TenantStorage, tenantID string) (haDedupWriter, error) {
	ds, ok := s.(haDedupTenantStorage)
	if !ok {
		return haDedupWriter{}, nil
	}
	tracker, err := ds.TenantHADedupTracker(tenantID)
	if err != nil || tracker == nil {
		return haDedupWriter{}, err
	}
	return haDedupWriter{tracker: tracker, horizon: tracker.Horizon(), changed: haDedupChangedPool.Get()}, nil
}

// prepare looks up the replica label of the given sorted labels. For deduplicated series it returns the labels
// without the replica label. With inPlace the given labels are modified, otherwise they are copied into a scratch
// buffer reused across the series of the request: remote-write requests share label memory between the local write
// and concurrent forwards to other endpoints, so it must not be modified then.
func (w *haDedupWriter) prepare(lbls []labelpb.ZLabel, inPlace bool) ([]labelpb.ZLabel, haDedupSeries) {
	if w.tracker == nil {
		return lbls, haDedupSeries{}
	}

	i, ok := slices.BinarySearchFunc(lbls, w.tracker.ReplicaLabel(), func(l labelpb.ZLabel, name string) int {
		return strings.Compare(l.Name, name)
	})
	if !ok {
		w.counts.NoLabel++
		return lbls, haDedupSeries{}
	}
	value := lbls[i].Value
	if len(value) > hadedup.MaxReplicaValueLength {
		w.counts.ReplicaValueTooLong++
		return lbls, haDedupSeries{}
	}
	replica, status := w.lookupReplica(value)
	if status == replicaTableFull {
		w.counts.ReplicaTableFull++
		return lbls, haDedupSeries{}
	}

	if inPlace {
		lbls = removeLabelAt(lbls[:0], lbls, i)
	} else {
		w.scratch = removeLabelAt(w.scratch, lbls, i)
		lbls = w.scratch
	}
	return lbls, haDedupSeries{dedup: true, replica: replica, replicaValue: value, interned: status == replicaInterned}
}

func (w *haDedupWriter) lookupReplica(value string) (uint16, replicaStatus) {
	if w.lastReplicaStatus == replicaNotLookedUp || value != w.lastReplicaValue {
		r, ok, full := w.tracker.LookupReplica(value)
		w.lastReplicaValue, w.lastReplica = value, r
		switch {
		case ok:
			w.lastReplicaStatus = replicaInterned
		case full:
			w.lastReplicaStatus = replicaTableFull
		default:
			w.lastReplicaStatus = replicaUnknown
		}
	}
	return w.lastReplica, w.lastReplicaStatus
}

// intern interns the replica value of a series once one of its samples takes part in a decision, so that series
// without valid samples can't fill the replica table. If the table filled up since prepare, the request must be
// aborted, see abort: the series must then be written with the replica label, which its labels no longer have.
func (w *haDedupWriter) intern(s *haDedupSeries) bool {
	if s.interned {
		return true
	}
	r, ok := w.tracker.Replica(s.replicaValue)
	if !ok {
		w.tableFilled = true
	}
	s.replica, s.interned = r, ok
	if w.lastReplicaStatus == replicaUnknown && w.lastReplicaValue == s.replicaValue {
		w.lastReplica = r
		w.lastReplicaStatus = replicaInterned
		if !ok {
			w.lastReplicaStatus = replicaTableFull
		}
	}
	return ok
}

// flush records the outcomes of the request's decisions in the tracker's metrics.
func (w *haDedupWriter) flush() {
	if w.tracker == nil {
		return
	}
	w.tracker.Record(w.counts)
	w.counts = hadedup.Counts{}
	if cap(w.changed) > 0 {
		haDedupChangedPool.Put(w.changed[:0])
	}
	w.changed = nil
}

// forget removes the state of the series that is based on samples of the request. It must be called if the request's
// samples are not stored, as the state would otherwise make other replicas' samples and a retry be dropped.
func (w *haDedupWriter) forget() {
	for _, ref := range w.changed {
		w.forgetSeries(ref)
	}
	w.changed = w.changed[:0]
}

// abort rolls back a request that can't be written as prepared, see intern. The returned error makes clients retry
// the request, which then finds the replica table full and writes the series with the replica label.
func (w *haDedupWriter) abort(app storage.Appender) error {
	w.forget()
	if err := app.Rollback(); err != nil {
		return errors.Wrap(err, "rollback samples")
	}
	return errors.Wrap(errNotReady, "HA replica table filled up while writing the request")
}

func (w *haDedupWriter) forgetSeries(ref storage.SeriesRef) {
	if w.tracker.Forget(ref) {
		w.counts.Forgotten++
	}
}

// removeLabelAt appends lbls without the label at index i to dst[:0]. dst may share memory with lbls.
func removeLabelAt(dst, lbls []labelpb.ZLabel, i int) []labelpb.ZLabel {
	dst = append(dst[:0], lbls[:i]...)
	return append(dst, lbls[i+1:]...)
}

// haDedupSeries is the deduplication state of a single series of a request. The zero value accepts everything.
type haDedupSeries struct {
	dedup   bool
	replica uint16
	// replicaValue references request memory. replica is only valid once interned.
	replicaValue string
	interned     bool

	// ref is the last known head reference of the series. Appends return a zero reference on error, which must not
	// make later samples of the series bypass the tracker.
	ref       storage.SeriesRef
	pending   bool
	pendingTs int64
	// decided is set while a sample accepted by the tracker is appended, see appended.
	decided bool
	// listed is set once the series is in haDedupWriter.changed.
	listed bool
}

// accept reports whether a sample at ts must be appended to the series with the given head reference.
// Every accepted sample must be followed by a call to appended with the reference returned by the append.
func (w *haDedupWriter) accept(s *haDedupSeries, ref storage.SeriesRef, ts int64, stale bool) bool {
	if !s.dedup {
		return true
	}
	if ref != 0 {
		s.ref = ref
	}
	if s.ref == 0 {
		// The series doesn't exist in the head yet, so there's no state to decide on: the appended sample elects
		// its replica once the series has a reference.
		s.pending, s.pendingTs = true, ts
		return true
	}
	if !w.intern(s) {
		return false
	}
	accept, change := w.tracker.Accept(s.ref, s.replica, ts, w.horizon, stale)
	if accept {
		w.counts.Accepted++
	} else {
		w.counts.Dropped++
	}
	switch change {
	case hadedup.Elected:
		w.counts.Elections++
	case hadedup.Failover:
		w.counts.Failovers++
	case hadedup.Handover:
		w.counts.Handovers++
	case hadedup.Takeover:
		w.counts.Takeovers++
	}
	if change != hadedup.NoChange && !accept {
		w.list(s)
	}
	s.decided = accept
	return accept
}

// appended must be called with the reference returned by appending an accepted sample, which is zero if the append
// failed. A rejected sample that the tracker decided on makes the series be forgotten, so that the state it advanced
// can't make later valid samples be dropped.
func (w *haDedupWriter) appended(s *haDedupSeries, ref storage.SeriesRef) {
	if !s.dedup {
		return
	}
	decided := s.decided
	s.decided = false
	if ref == 0 {
		if decided {
			w.forgetSeries(s.ref)
		}
		return
	}
	s.ref = ref
	if s.pending {
		s.pending = false
		if !w.intern(s) {
			return
		}
		w.counts.Accepted++
		if !w.tracker.Init(ref, s.replica, s.pendingTs, w.horizon) {
			return
		}
		w.counts.Elections++
	}
	w.list(s)
}

func (w *haDedupWriter) list(s *haDedupSeries) {
	if !s.listed {
		s.listed = true
		w.changed = append(w.changed, s.ref)
	}
}

// acceptExemplars reports whether exemplars of the series with the given head reference must be appended.
func (w *haDedupWriter) acceptExemplars(s *haDedupSeries, ref storage.SeriesRef) bool {
	if !s.dedup {
		return true
	}
	if !s.interned {
		// A replica that is not interned owns no series, and its index is not valid.
		return !w.tracker.Tracked(ref)
	}
	return w.tracker.IsOwner(ref, s.replica)
}

func isStaleHistogram(h *histogram.Histogram, fh *histogram.FloatHistogram) bool {
	if h != nil {
		return value.IsStaleNaN(h.Sum)
	}
	return fh != nil && value.IsStaleNaN(fh.Sum)
}
