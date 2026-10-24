// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/gogo/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestPrometheusStore_Series_MemoryMetadata(t *testing.T) {
	const twoRows = `[{"job":"api","series":"a"},{"job":"api","series":"b"}]`
	for _, tc := range []struct {
		name, body, diagnostic string
		wantSeries             []string
		wantErr                bool
		drops                  float64
		limit                  uint64
		code                   int
		readErr, closeErr      error
		canceled               bool
	}{
		{name: "success sorts", body: `{"data":[{"job":"api","series":"b"},{"job":"api","series":"a"}],"status":"success"}`, limit: 2, wantSeries: []string{"a", "b"}},
		{name: "case insensitive envelope", body: `{"DATA":[{"job":"api","series":"a"}],"STATUS":"success"}`, wantSeries: []string{"a"}},
		{name: "valid overflow", body: `{"data":` + twoRows + `,"status":"success"}`, wantErr: true, drops: 1, diagnostic: "failed to send series"},
		{name: "status failure after overflow", body: `{"data":` + twoRows + `,"status":"error","error":"upstream rejected"}`, wantErr: true, diagnostic: "upstream rejected"},
		{name: "malformed after overflow", body: `{"data":` + twoRows + `,"status":"success"`, wantErr: true},
		{name: "trailing json after overflow", body: `{"data":` + twoRows + `,"status":"success"} {}`, wantErr: true},
		{name: "later typed row error after overflow", body: `{"data":[{"job":"api","series":"a"},{"job":"api","series":"b"},{"job":true}],"status":"success"}`, wantErr: true, diagnostic: "cannot unmarshal"},
		{name: "duplicate data resets overflow and budget", body: `{"data":` + twoRows + `,"data":[{"job":"api","series":"kept"}],"status":"success"}`, wantSeries: []string{"kept"}},
		{name: "duplicate null clears overflow", body: `{"data":` + twoRows + `,"data":null,"status":"success"}`},
		{name: "earlier typed error survives duplicate replacement", body: `{"data":[{"job":true}],"data":[{"job":"api","series":"kept"}],"status":"success"}`, wantErr: true, diagnostic: "cannot unmarshal"},
		{name: "status failure before typed error", body: `{"data":[{"job":true}],"status":"error","error":"upstream rejected"}`, wantErr: true, diagnostic: "upstream rejected"},
		{name: "read failure after complete json", body: `{"data":` + twoRows + `,"status":"success"}`, wantErr: true, readErr: errors.New("read stopped"), diagnostic: "read stopped"},
		{name: "close failure before overflow", body: `{"data":` + twoRows + `,"status":"success"}`, wantErr: true, closeErr: errors.New("close stopped"), diagnostic: "close stopped"},
		{name: "close failure after successful data", body: `{"data":[{"job":"api","series":"a"}],"status":"success"}`, wantErr: true, closeErr: errors.New("close stopped"), diagnostic: "close stopped"},
		{name: "http failure before parsing", body: `{"data":` + twoRows + `,"status":"success"}`, code: http.StatusNotFound, wantErr: true, diagnostic: "expected 2xx"},
		{name: "empty response", code: http.StatusNoContent},
		{name: "cancellation closes body", body: `{"data":` + twoRows + `,"status":"success"}`, canceled: true, wantErr: true, diagnostic: context.Canceled.Error()},
	} {
		for _, batchSize := range []int64{0, 1, 64} {
			t.Run(fmt.Sprintf("%s/batch_%d", tc.name, batchSize), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.canceled {
					cancel()
				}
				body := &metadataStreamBody{data: []byte(tc.body), readErr: tc.readErr, closeErr: tc.closeErr, ctx: ctx}
				code := tc.code
				if code == 0 {
					code = http.StatusOK
				}
				limit := tc.limit
				if limit == 0 {
					limit = 1
				}
				reg := prometheus.NewRegistry()
				store := newEarlyLimitsStoreWithClient(t, reg, SeriesSelectLimits{SeriesPerRequest: limit, SamplesPerRequest: 1}, labels.FromStrings("cluster", "metadata-test"), roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: code, Header: make(http.Header), Body: body}, nil
				}))
				srv := newStoreSeriesServer(ctx)
				err := store.Series(&storepb.SeriesRequest{
					SkipChunks:        true,
					ResponseBatchSize: batchSize,
					Matchers:          []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}},
				}, srv)
				if (err != nil) != tc.wantErr {
					t.Fatalf("error=%v, wantErr=%t", err, tc.wantErr)
				}
				if tc.wantErr {
					testutil.Equals(t, 0, len(srv.SeriesSet))
					if tc.diagnostic != "" && !strings.Contains(err.Error(), tc.diagnostic) {
						t.Fatalf("error %v does not contain %q", err, tc.diagnostic)
					}
					if code == http.StatusNotFound {
						testutil.Equals(t, codes.NotFound, status.Code(err))
					}
				} else {
					testutil.Equals(t, len(tc.wantSeries), len(srv.SeriesSet))
					for i, want := range tc.wantSeries {
						assertLabelValue(t, srv.SeriesSet[i].Labels, "series", want)
						assertLabelValue(t, srv.SeriesSet[i].Labels, "cluster", "metadata-test")
						testutil.Equals(t, 0, len(srv.SeriesSet[i].Chunks))
					}
				}
				testutil.Equals(t, tc.drops, selectDropCount(t, reg, "series"))
				testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
				testutil.Assert(t, body.closed, "metadata response body was not closed")
				if !tc.canceled {
					testutil.Equals(t, len(body.data), body.position)
				}
			})
		}
	}
	t.Run("duplicate data replaces labels instead of merging", func(t *testing.T) {
		const payload = `{"data":[{"job":"api","series":"old","extra":"keep"}],"data":[{"job":"api","series":"new"}],"status":"success"}`
		for _, batchSize := range []int64{0, 1, 64} {
			t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
				body := &metadataStreamBody{data: []byte(payload)}
				reg := prometheus.NewRegistry()
				store := newEarlyLimitsStoreWithClient(t, reg, SeriesSelectLimits{SeriesPerRequest: 1}, labels.FromStrings("cluster", "metadata-test"), roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
				}))
				srv := newStoreSeriesServer(context.Background())
				testutil.Ok(t, store.Series(&storepb.SeriesRequest{
					SkipChunks:        true,
					ResponseBatchSize: batchSize,
					Matchers:          []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}},
				}, srv))
				testutil.Equals(t, 1, len(srv.SeriesSet))
				assertLabelValue(t, srv.SeriesSet[0].Labels, "cluster", "metadata-test")
				assertLabelValue(t, srv.SeriesSet[0].Labels, "job", "api")
				assertLabelValue(t, srv.SeriesSet[0].Labels, "series", "new")
				assertNoLabel(t, srv.SeriesSet[0].Labels, "extra")
				testutil.Equals(t, 3, len(srv.SeriesSet[0].Labels))
				testutil.Equals(t, "cluster", srv.SeriesSet[0].Labels[0].Name)
				testutil.Equals(t, "job", srv.SeriesSet[0].Labels[1].Name)
				testutil.Equals(t, "series", srv.SeriesSet[0].Labels[2].Name)
				testutil.Equals(t, 0, len(srv.SeriesSet[0].Chunks))
				testutil.Equals(t, 0.0, selectDropCount(t, reg, "series"))
				testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
				testutil.Equals(t, len(body.data), body.position)
				testutil.Assert(t, body.closed, "metadata response body was not closed")
			})
		}
	})

}

type metadataStreamBody struct {
	data              []byte
	position          int
	closed            bool
	readErr, closeErr error
	ctx               context.Context
}

func (b *metadataStreamBody) Read(p []byte) (int, error) {
	if b.ctx != nil && b.ctx.Err() != nil {
		return 0, b.ctx.Err()
	}
	if b.position == len(b.data) {
		if b.readErr != nil {
			return 0, b.readErr
		}
		return 0, io.EOF
	}
	end := min(b.position+17, len(b.data))
	n := copy(p, b.data[b.position:end])
	b.position += n
	return n, nil
}

func (b *metadataStreamBody) Close() error {
	b.closed = true
	return b.closeErr
}
