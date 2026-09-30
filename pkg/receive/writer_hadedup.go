// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"slices"
	"strings"

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

// haDedupWriter applies HA replica deduplication to the series of a single write request.
// It is shared by Writer and CapNProtoWriter so that both paths take the same decisions.
type haDedupWriter struct {
	tracker *hadedup.Tracker
	scratch []labelpb.ZLabel
	// ownerChanged lists the series whose owner was changed by the request, see forget.
	ownerChanged []storage.SeriesRef
	counts       hadedup.Counts

	// The series of a request normally come from a single Prometheus, so the lookup of the last replica value is
	// reused while it repeats. The value references request memory without being copied: the request's label
	// strings stay unmodified until the request is written, prepare with inPlace only moves label headers.
	lastReplicaValue string
	lastReplica      uint16
	lastReplicaOK    bool
	lastReplicaSet   bool
}

func newHADedupWriter(s TenantStorage, tenantID string) (haDedupWriter, error) {
	ds, ok := s.(haDedupTenantStorage)
	if !ok {
		return haDedupWriter{}, nil
	}
	tracker, err := ds.TenantHADedupTracker(tenantID)
	return haDedupWriter{tracker: tracker}, err
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
	replica, ok := w.replica(lbls[i].Value)
	if !ok {
		w.counts.ReplicaTableFull++
		return lbls, haDedupSeries{}
	}

	if inPlace {
		lbls = removeLabelAt(lbls[:0], lbls, i)
	} else {
		w.scratch = removeLabelAt(w.scratch, lbls, i)
		lbls = w.scratch
	}
	return lbls, haDedupSeries{dedup: true, replica: replica}
}

func (w *haDedupWriter) replica(value string) (uint16, bool) {
	if !w.lastReplicaSet || value != w.lastReplicaValue {
		w.lastReplica, w.lastReplicaOK = w.tracker.Replica(value)
		w.lastReplicaValue, w.lastReplicaSet = value, true
	}
	return w.lastReplica, w.lastReplicaOK
}

// flush records the outcomes of the request's decisions in the tracker's metrics.
func (w *haDedupWriter) flush() {
	if w.tracker == nil {
		return
	}
	w.tracker.Record(w.counts)
	w.counts = hadedup.Counts{}
}

// forget removes the state of the series whose owner was changed by the request. It must be called if the request's
// samples are not stored, as a retry would otherwise be dropped by the ownership they established.
func (w *haDedupWriter) forget() {
	for _, ref := range w.ownerChanged {
		w.tracker.Forget(ref)
	}
	w.ownerChanged = nil
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

	// ref is the last known head reference of the series. Appends return a zero reference on error, which must not
	// make later samples of the series bypass the tracker.
	ref          storage.SeriesRef
	pending      bool
	pendingTs    int64
	ownerChanged bool
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
	accept, failover, ownerChanged := w.tracker.Accept(s.ref, s.replica, ts, stale)
	if accept {
		w.counts.Accepted++
	} else {
		w.counts.Dropped++
	}
	if failover {
		w.counts.Failovers++
	}
	if ownerChanged && !accept {
		w.ownerChanged = append(w.ownerChanged, s.ref)
	}
	s.ownerChanged = ownerChanged && accept
	return accept
}

// appended must be called with the reference returned by appending an accepted sample, which is zero if the append
// failed. A rejected sample that changed the owner makes the series be forgotten, so that it can't make later valid
// samples be dropped.
func (w *haDedupWriter) appended(s *haDedupSeries, ref storage.SeriesRef) {
	if !s.dedup {
		return
	}
	if ref == 0 {
		if s.ownerChanged {
			w.tracker.Forget(s.ref)
			s.ownerChanged = false
		}
		return
	}
	s.ref = ref
	if s.pending {
		s.ownerChanged = w.tracker.Init(ref, s.replica, s.pendingTs)
		s.pending = false
		w.counts.Accepted++
	}
	if s.ownerChanged {
		w.ownerChanged = append(w.ownerChanged, ref)
		s.ownerChanged = false
	}
}

// acceptExemplars reports whether exemplars of the series with the given head reference must be appended.
func (w *haDedupWriter) acceptExemplars(s *haDedupSeries, ref storage.SeriesRef) bool {
	return !s.dedup || w.tracker.IsOwner(ref, s.replica)
}

func isStaleHistogram(h *histogram.Histogram, fh *histogram.FloatHistogram) bool {
	if h != nil {
		return value.IsStaleNaN(h.Sum)
	}
	return fh != nil && value.IsStaleNaN(fh.Sum)
}
