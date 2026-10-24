// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package promclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
)

func TestGet2xxMetadataResponses(t *testing.T) {
	base, err := url.Parse("http://prometheus.test")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, status, body string
		wantErr            bool
	}{
		{name: "success non-series helper", status: "200 OK", body: `{"status":"success","data":{"groups":[]}}`},
		{name: "http failure", status: "500 Internal Server Error", body: `upstream failed`, wantErr: true},
		{name: "prometheus status failure", status: "200 OK", body: `{"status":"error","error":"bad query","data":null}`, wantErr: true},
		{name: "malformed envelope", status: "200 OK", body: `{"status":`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := metadataBehaviorClient(tc.status, tc.body)
			_, err := client.RulesInGRPC(context.Background(), base, "")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v, wantErr=%t", err, tc.wantErr)
			}
		})
	}
	if got, err := metadataBehaviorClient("204 No Content", "").SeriesInGRPC(context.Background(), base, nil, 0, 1, 0); err != nil || got != nil {
		t.Fatalf("SeriesInGRPC on 204 = %v, %v; want nil, nil", got, err)
	}
}

func TestSeriesMetadataResponses(t *testing.T) {
	base, err := url.Parse("http://prometheus.test")
	if err != nil {
		t.Fatal(err)
	}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "job", "api")
	client := metadataBehaviorClient("200 OK", `{"status":"success","data":[{"job":"api","instance":"a"},{"job":"api","instance":"b"}]}`)
	got, err := client.SeriesInGRPC(context.Background(), base, []*labels.Matcher{matcher}, 0, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0]["instance"] != "a" || got[1]["instance"] != "b" {
		t.Fatalf("unexpected series response: %#v", got)
	}
	for _, tc := range []struct {
		body   string
		status string
	}{
		{`{"status":"success","data":{}}`, "200 OK"},
		{`{"status":"success","data":[`, "200 OK"},
		{`{"status":"error","error":"query rejected"}`, "200 OK"},
		{`not json`, "502 Bad Gateway"},
	} {
		got, err := metadataBehaviorClient(tc.status, tc.body).SeriesInGRPC(context.Background(), base, []*labels.Matcher{matcher}, 0, 1, 0)
		if err == nil || got != nil {
			t.Fatalf("body=%q status=%q returned %#v, %v; want nil result and error", tc.body, tc.status, got, err)
		}
	}
	var fixture strings.Builder
	fixture.WriteString(`{"status":"success","data":[`)
	for i := 0; i < 10; i++ {
		if i > 0 {
			fixture.WriteByte(',')
		}
		fmt.Fprintf(&fixture, `{"__name__":"metric_%d","instance":"host-%d","job":"benchmark"}`, i, i)
	}
	fixture.WriteString(`]}`)
	allocClient := metadataBehaviorClient("200 OK", fixture.String())
	allocMatcher := labels.MustNewMatcher(labels.MatchEqual, "job", "benchmark")
	if got, err := allocClient.SeriesInGRPC(context.Background(), base, []*labels.Matcher{allocMatcher}, 0, 1, 0); err != nil || len(got) != 10 {
		t.Fatalf("allocation warmup returned %d series, err=%v", len(got), err)
	}
	if allocations := testing.AllocsPerRun(20, func() {
		if _, err := allocClient.SeriesInGRPC(context.Background(), base, []*labels.Matcher{allocMatcher}, 0, 1, 0); err != nil {
			panic(err)
		}
	}); allocations > 250 {
		t.Fatalf("metadata series request allocated %.0f objects/op, want <= 250 (baseline fixture was 287)", allocations)
	}
}

func TestLabelMetadataResponses(t *testing.T) {
	base, err := url.Parse("http://prometheus.test")
	if err != nil {
		t.Fatal(err)
	}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "job", "api")
	client := metadataBehaviorClient("200 OK", `{"status":"success","data":["instance","job"]}`)
	names, err := client.LabelNamesInGRPC(context.Background(), base, []*labels.Matcher{matcher}, 0, 1, 0)
	if err != nil || len(names) != 2 || names[0] != "instance" || names[1] != "job" {
		t.Fatalf("LabelNamesInGRPC() = %v, %v", names, err)
	}
	valuesClient := metadataBehaviorClient("200 OK", `{"status":"success","data":["a","b"]}`)
	values, err := valuesClient.LabelValuesInGRPC(context.Background(), base, "instance", []*labels.Matcher{matcher}, 0, 1, 0)
	if err != nil || len(values) != 2 || values[0] != "a" || values[1] != "b" {
		t.Fatalf("LabelValuesInGRPC() = %v, %v", values, err)
	}
	for _, body := range []string{`{"status":"success","data":{}}`, `{"status":"success","data":[`, `{"status":"error","error":"bad query"}`} {
		got, err := metadataBehaviorClient("200 OK", body).LabelNamesInGRPC(context.Background(), base, nil, 0, 1, 0)
		if err == nil || got != nil {
			t.Fatalf("body=%q returned %v, %v; want nil result and error", body, got, err)
		}
	}
}

func metadataBehaviorClient(status, body string) *Client {
	return NewClient(metadataMemoryRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{Status: status, StatusCode: statusCode(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	}), log.NewNopLogger(), "")
}

func statusCode(status string) int {
	var code int
	if _, err := fmt.Sscanf(status, "%d", &code); err != nil {
		panic(err)
	}
	return code
}
