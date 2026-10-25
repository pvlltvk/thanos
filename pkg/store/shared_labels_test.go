// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type sharedLabelCase struct {
	name                      string
	local, external           labels.Labels
	remove                    map[string]struct{}
	tsdb, gateway, prometheus labels.Labels
}

func sharedLabelCases() []sharedLabelCase {
	base := labels.FromStrings("cluster", "local", "empty", "", "job", "api")
	trimmed := labels.FromStrings("cluster", "local", "job", "api")
	return []sharedLabelCase{
		{name: "overlay and removal", local: labels.FromStrings("a", "first", "cluster", "local", "job", "api", "replica", "local", "z", "last"), external: labels.FromStrings("cluster", "external", "ext", "value", "replica", "external"), remove: map[string]struct{}{"replica": {}}, tsdb: labels.FromStrings("a", "first", "cluster", "external", "ext", "value", "job", "api", "z", "last"), gateway: labels.FromStrings("a", "first", "cluster", "external", "ext", "value", "job", "api", "z", "last"), prometheus: labels.FromStrings("a", "first", "cluster", "external", "ext", "value", "job", "api", "z", "last")},
		{name: "interleaved labels", local: labels.FromStrings("a", "1", "c", "3", "job", "api"), external: labels.FromStrings("b", "2", "d", "4"), tsdb: labels.FromStrings("a", "1", "b", "2", "c", "3", "d", "4", "job", "api"), gateway: labels.FromStrings("a", "1", "b", "2", "c", "3", "d", "4", "job", "api"), prometheus: labels.FromStrings("a", "1", "b", "2", "c", "3", "d", "4", "job", "api")},
		{name: "nil removal with empty values", local: base, tsdb: trimmed, gateway: base, prometheus: trimmed},
		{name: "empty removal with empty values", local: base, remove: map[string]struct{}{}, tsdb: trimmed, gateway: trimmed, prometheus: trimmed},
		{name: "missing removal still trims", local: base, remove: map[string]struct{}{"absent": {}}, tsdb: trimmed, gateway: trimmed, prometheus: trimmed},
		{name: "empty external value", local: trimmed, external: labels.FromStrings("cluster", ""), tsdb: trimmed, gateway: labels.FromStrings("job", "api"), prometheus: labels.FromStrings("job", "api")},
		{name: "empty external value with empty removal", local: trimmed, external: labels.FromStrings("cluster", ""), remove: map[string]struct{}{}, tsdb: trimmed, gateway: trimmed, prometheus: labels.FromStrings("job", "api")},
		{name: "remove external and local", local: trimmed, external: labels.FromStrings("cluster", "external"), remove: map[string]struct{}{"cluster": {}}, tsdb: labels.FromStrings("job", "api"), gateway: labels.FromStrings("job", "api"), prometheus: labels.FromStrings("job", "api")},
	}
}

func TestSharedStoreLabelTransform(t *testing.T) {
	for _, tc := range sharedLabelCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, caller := range []string{"prometheus", "tsdb", "gateway"} {
				t.Run(caller, func(t *testing.T) {
					local, external := tc.local.Copy(), tc.external.Copy()
					var builder labels.Builder
					got := sharedLabelTransform(&builder, caller, local, external, tc.remove)
					want := tc.prometheus
					if caller == "tsdb" {
						want = tc.tsdb
					}
					if caller == "gateway" {
						want = tc.gateway
					}
					testutil.Equals(t, want, got)
					testutil.Equals(t, tc.local, local)
					testutil.Equals(t, tc.external.Copy(), external)
					before := got.Copy()
					_ = sharedLabelTransform(&builder, caller, labels.FromStrings("job", "another", "z", "next"), external, tc.remove)
					testutil.Equals(t, before, got)
					if len(local) > 0 {
						local[0].Value = "changed input"
					}
					testutil.Equals(t, before, got)
				})
			}
		})
	}
}

// Model caller preprocessing to isolate the helper; separate handler/client tests cover production callers.
func sharedLabelTransform(builder *labels.Builder, caller string, local, external labels.Labels, remove map[string]struct{}) labels.Labels {
	switch caller {
	case "tsdb":
		external = rmLabels(external.Copy(), remove)
	case "gateway":
		if remove != nil {
			external = rmLabels(external.Copy(), remove)
		}
		if external.IsEmpty() && remove == nil {
			return local.Copy()
		}
	}
	return transformStoreLabels(builder, local, external, remove)
}

func TestSharedStoreTSDBLabels(t *testing.T) {
	for _, tc := range sharedLabelCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, batch := range []int64{0, 1, 64} {
				inputs := []labels.Labels{tc.local.Copy(), tc.local.Copy()}
				db := &sharedLabelDB{sets: inputs}
				store := NewTSDBStore(nil, db, component.Receive, tc.external.Copy())
				srv := newStoreSeriesServer(t.Context())
				var remove []string
				for name := range tc.remove {
					remove = append(remove, name)
				}
				testutil.Ok(t, store.Series(&storepb.SeriesRequest{MinTime: 0, MaxTime: 10, SkipChunks: true, ResponseBatchSize: batch, Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}, WithoutReplicaLabels: remove}, srv))
				testutil.Equals(t, 2, len(srv.SeriesSet))
				for _, series := range srv.SeriesSet {
					testutil.Equals(t, tc.tsdb, labelpb.ZLabelsToPromLabels(series.Labels))
				}
				before := append([]labelpb.ZLabel(nil), srv.SeriesSet[1].Labels...)
				srv.SeriesSet[0].Labels[0].Value = "changed output"
				testutil.Equals(t, before, srv.SeriesSet[1].Labels)
				testutil.Equals(t, tc.local, inputs[0])
			}
		})
	}
}

type sharedLabelDB struct{ sets []labels.Labels }

func (*sharedLabelDB) StartTime() (int64, error) { return 0, nil }
func (d *sharedLabelDB) ChunkQuerier(int64, int64) (storage.ChunkQuerier, error) {
	return &sharedLabelQuerier{sets: d.sets}, nil
}

type sharedLabelQuerier struct {
	storage.ChunkQuerier
	sets []labels.Labels
}

func (*sharedLabelQuerier) Close() error { return nil }
func (q *sharedLabelQuerier) Select(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.ChunkSeriesSet {
	return &sharedLabelSet{sets: q.sets, index: -1}
}

type sharedLabelSet struct {
	sets  []labels.Labels
	index int
}

func (s *sharedLabelSet) Next() bool                      { s.index++; return s.index < len(s.sets) }
func (s *sharedLabelSet) At() storage.ChunkSeries         { return sharedLabelSeries{lset: s.sets[s.index]} }
func (*sharedLabelSet) Err() error                        { return nil }
func (*sharedLabelSet) Warnings() annotations.Annotations { return nil }

type sharedLabelSeries struct {
	storage.ChunkSeries
	lset labels.Labels
}

func (s sharedLabelSeries) Labels() labels.Labels { return s.lset }
