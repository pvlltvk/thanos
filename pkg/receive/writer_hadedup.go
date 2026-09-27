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
	TenantHADedupTracker(tenantID string) *hadedup.Tracker
}

// haDedupWriter applies HA replica deduplication to the series of a single write request.
// It is shared by Writer and CapNProtoWriter so that both paths take the same decisions.
type haDedupWriter struct {
	tracker *hadedup.Tracker
	scratch []labelpb.ZLabel
}

func newHADedupWriter(s TenantStorage, tenantID string) haDedupWriter {
	ds, ok := s.(haDedupTenantStorage)
	if !ok {
		return haDedupWriter{}
	}
	return haDedupWriter{tracker: ds.TenantHADedupTracker(tenantID)}
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
		w.tracker.Passthrough(hadedup.PassthroughReasonNoLabel)
		return lbls, haDedupSeries{}
	}
	replica, ok := w.tracker.Replica(lbls[i].Value)
	if !ok {
		w.tracker.Passthrough(hadedup.PassthroughReasonReplicaTableFull)
		return lbls, haDedupSeries{}
	}

	if inPlace {
		lbls = removeLabelAt(lbls[:0], lbls, i)
	} else {
		w.scratch = removeLabelAt(w.scratch, lbls, i)
		lbls = w.scratch
	}
	return lbls, haDedupSeries{tracker: w.tracker, replica: replica}
}

// removeLabelAt appends lbls without the label at index i to dst[:0]. dst may share memory with lbls.
func removeLabelAt(dst, lbls []labelpb.ZLabel, i int) []labelpb.ZLabel {
	dst = append(dst[:0], lbls[:i]...)
	return append(dst, lbls[i+1:]...)
}

// haDedupSeries takes deduplication decisions for the samples of a single series. The zero value accepts everything.
type haDedupSeries struct {
	tracker *hadedup.Tracker
	replica uint16

	// ref is the last known head reference of the series. Appends return a zero reference on error, which must not
	// make later samples of the series bypass the tracker.
	ref       storage.SeriesRef
	pending   bool
	pendingTs int64
}

// accept reports whether a sample at ts must be appended to the series with the given head reference.
// Every accepted sample must be followed by a call to appended with the reference returned by the append.
func (s *haDedupSeries) accept(ref storage.SeriesRef, ts int64, stale bool) bool {
	if s.tracker == nil {
		return true
	}
	if ref == 0 {
		ref = s.ref
	}
	if ref == 0 {
		// The series doesn't exist in the head yet, so there's no state to decide on: the appended sample elects
		// its replica once the series has a reference.
		s.pending, s.pendingTs = true, ts
		return true
	}
	return s.tracker.Accept(ref, s.replica, ts, stale)
}

func (s *haDedupSeries) appended(ref storage.SeriesRef) {
	if s.tracker == nil || ref == 0 {
		return
	}
	s.ref = ref
	if s.pending {
		s.tracker.Init(ref, s.replica, s.pendingTs)
		s.pending = false
	}
}

// acceptExemplars reports whether exemplars of the series must be appended.
func (s *haDedupSeries) acceptExemplars(ref storage.SeriesRef) bool {
	return s.tracker == nil || s.tracker.IsOwner(ref, s.replica)
}

func isStaleHistogram(h *histogram.Histogram, fh *histogram.FloatHistogram) bool {
	if h != nil {
		return value.IsStaleNaN(h.Sum)
	}
	return fh != nil && value.IsStaleNaN(fh.Sum)
}
