// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package promclient

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestSeriesInGRPCStreamErrorMessages(t *testing.T) {
	for _, body := range []string{"", `{"status":"success","data":[`, `{"status":"success","data":[]} {}`, `{"status":"success","data":[]} true`, `{"status":1}`, `{"status":"success","error":1}`, `{"status":"success","data":{}}`, `{"status":"success","data":[1]}`, `{"status":"success","data":[{"job":true}]}`, `[]`, `true`} {
		t.Run(body, func(t *testing.T) {
			base := &url.URL{Scheme: "http", Host: "prometheus.test"}
			legacy := seriesStreamTestClient(&seriesStreamTestBody{data: []byte(body), chunk: 3}, http.StatusOK, nil)
			_, want := legacy.SeriesInGRPC(context.Background(), base, nil, 0, 1, 0)
			stream := seriesStreamTestClient(&seriesStreamTestBody{data: []byte(body), chunk: 3}, http.StatusOK, nil)
			got := stream.SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0, nil, nil)
			testutil.Assert(t, want != nil && got != nil, "expected both APIs to reject %q", body)
			testutil.Equals(t, want.Error(), got.Error())
		})
	}
}

func TestSeriesInGRPCStreamDepth(t *testing.T) {
	for _, tc := range []struct {
		name    string
		depth   int
		unknown bool
	}{
		{name: "root at limit", depth: 10000},
		{name: "root beyond limit", depth: 10001},
		{name: "unknown at limit", depth: 9999, unknown: true},
		{name: "unknown beyond limit", depth: 10000, unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Repeat("[", tc.depth) + "0" + strings.Repeat("]", tc.depth)
			if tc.unknown {
				body = `{"status":"success","unknown":` + body + `,"data":[]}`
			}
			base := &url.URL{Scheme: "http", Host: "prometheus.test"}
			legacy := seriesStreamTestClient(&seriesStreamTestBody{data: []byte(body), chunk: 23}, http.StatusOK, nil)
			_, want := legacy.SeriesInGRPC(context.Background(), base, nil, 0, 1, 0)
			stream := seriesStreamTestClient(&seriesStreamTestBody{data: []byte(body), chunk: 23}, http.StatusOK, nil)
			got := stream.SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0, nil, nil)
			if want == nil {
				testutil.Ok(t, got)
				return
			}
			testutil.Assert(t, got != nil, "stream accepted invalid body")
			testutil.Equals(t, want.Error(), got.Error())
		})
	}
}
