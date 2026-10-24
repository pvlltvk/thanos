// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestResortingServerRelease(t *testing.T) {
	t.Run("successful handoff releases sorter references", func(t *testing.T) {
		var sorter *resortingServer
		upstream := &releaseTestServer{beforeSend: func(send int) {
			if send == 2 && (len(sorter.series) == 0 || sorter.series[0] != nil) {
				t.Errorf("first sorter slot still retains the handed-off series before the second send")
			}
		}}
		sorter = &resortingServer{Store_SeriesServer: upstream}
		second := releaseTestSeries("b")
		first := releaseTestSeries("a")
		testutil.Ok(t, sorter.Send(storepb.NewSeriesResponse(second)))
		testutil.Ok(t, sorter.Send(storepb.NewSeriesResponse(first)))
		testutil.Ok(t, sorter.Flush())
		if len(sorter.series) != 0 {
			t.Fatalf("sorter retains %d series after successful handoff", len(sorter.series))
		}
		if len(upstream.responses) != 2 {
			t.Fatalf("got %d responses, want 2", len(upstream.responses))
		}
		testutil.Equals(t, "a", upstream.responses[0].GetSeries().Labels[0].Value)
		testutil.Equals(t, "b", upstream.responses[1].GetSeries().Labels[0].Value)
		testutil.Equals(t, first, upstream.responses[0].GetSeries())
		testutil.Equals(t, second, upstream.responses[1].GetSeries())
	})

	t.Run("batch handoff retains output ownership and releases sorter", func(t *testing.T) {
		upstream := &releaseTestServer{}
		batcher := &batchableServer{Store_SeriesServer: upstream, batchSize: 2, series: make([]*storepb.Series, 0, 2)}
		sorter := &resortingServer{Store_SeriesServer: batcher}
		second := releaseTestSeries("b")
		first := releaseTestSeries("a")
		testutil.Ok(t, sorter.Send(storepb.NewSeriesResponse(second)))
		testutil.Ok(t, sorter.Send(storepb.NewSeriesResponse(first)))
		testutil.Ok(t, sorter.Flush())
		if len(sorter.series) != 0 {
			t.Fatalf("sorter retains %d series after batch handoff", len(sorter.series))
		}
		if len(upstream.responses) != 1 || upstream.responses[0].GetBatch() == nil {
			t.Fatalf("got responses %#v, want one batch", upstream.responses)
		}
		batch := upstream.responses[0].GetBatch().Series
		if len(batch) != 2 {
			t.Fatalf("got batch of %d series, want 2", len(batch))
		}
		testutil.Equals(t, first, batch[0])
		testutil.Equals(t, second, batch[1])
	})

	t.Run("send failure releases buffered references", func(t *testing.T) {
		wantErr := errors.New("downstream stopped")
		upstream := &releaseTestServer{err: wantErr}
		sorter := &resortingServer{Store_SeriesServer: upstream}
		testutil.Ok(t, sorter.Send(storepb.NewSeriesResponse(releaseTestSeries("a"))))
		testutil.Ok(t, sorter.Send(storepb.NewSeriesResponse(releaseTestSeries("b"))))
		testutil.Assert(t, errors.Is(sorter.Flush(), wantErr), "expected downstream send error")
		if len(sorter.series) != 0 {
			t.Fatalf("sorter retains %d series after send failure", len(sorter.series))
		}
	})

	t.Run("notSend preserves SeriesLocal result", func(t *testing.T) {
		upstream := &releaseTestServer{}
		sorter := &resortingServer{Store_SeriesServer: upstream, notSend: true}
		second := releaseTestSeries("b")
		first := releaseTestSeries("a")
		testutil.Ok(t, sorter.Send(storepb.NewSeriesResponse(second)))
		testutil.Ok(t, sorter.Send(storepb.NewSeriesResponse(first)))
		testutil.Ok(t, sorter.Flush())
		testutil.Equals(t, 0, len(upstream.responses))
		if len(sorter.series) != 2 {
			t.Fatalf("notSend sorter retained %d series, want 2", len(sorter.series))
		}
		testutil.Equals(t, first, sorter.series[0])
		testutil.Equals(t, second, sorter.series[1])
	})
}

type releaseTestServer struct {
	storepb.Store_SeriesServer
	responses  []*storepb.SeriesResponse
	err        error
	beforeSend func(int)
}

func (s *releaseTestServer) Send(response *storepb.SeriesResponse) error {
	if s.beforeSend != nil {
		s.beforeSend(len(s.responses) + 1)
	}
	if s.err != nil {
		return s.err
	}
	s.responses = append(s.responses, response)
	return nil
}

func (s *releaseTestServer) Context() context.Context { return context.Background() }

func releaseTestSeries(value string) *storepb.Series {
	return &storepb.Series{Labels: []labelpb.ZLabel{{Name: "series", Value: value}}}
}
