// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package promclient

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
)

type metadataMemoryRoundTripper func(*http.Request) (*http.Response, error)

func (f metadataMemoryRoundTripper) Do(r *http.Request) (*http.Response, error) { return f(r) }

func BenchmarkPrometheusMetadataDecode(b *testing.B) {
	for _, count := range []int{10, 1000, 100000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			var fixture strings.Builder
			fixture.WriteString(`{"status":"success","data":[`)
			for i := 0; i < count; i++ {
				if i > 0 {
					fixture.WriteByte(',')
				}
				fmt.Fprintf(&fixture, `{"__name__":"metric_%d","instance":"host-%d","job":"benchmark"}`, i, i)
			}
			fixture.WriteString(`]}`)
			body := fixture.String()
			client := NewClient(metadataMemoryRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			}), log.NewNopLogger(), "")
			base, err := url.Parse("http://prometheus.test")
			if err != nil {
				b.Fatal(err)
			}
			matcher := labels.MustNewMatcher(labels.MatchEqual, "job", "benchmark")
			b.SetBytes(int64(len(body)))
			b.ReportMetric(float64(len(body)), "fixture_bytes/op")
			b.ReportAllocs()
			warm, err := client.SeriesInGRPC(context.Background(), base, []*labels.Matcher{matcher}, 0, 1, 0)
			if err != nil {
				b.Fatal(err)
			}
			wantDigest := metadataLabelsDigest(warm)
			if len(warm) != count || wantDigest == 0 {
				b.Fatalf("warmup got %d labels and digest %d, want %d and nonzero", len(warm), wantDigest, count)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := client.SeriesInGRPC(context.Background(), base, []*labels.Matcher{matcher}, 0, 1, 0)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != count || metadataLabelsDigest(got) != wantDigest {
					b.Fatalf("got %d labels, want %d with digest %d", len(got), count, wantDigest)
				}
			}
		})
	}
}

func metadataLabelsDigest(series []map[string]string) uint64 {
	h := fnv.New64a()
	for _, labels := range series {
		for _, name := range []string{"__name__", "instance", "job"} {
			_, _ = h.Write([]byte(name))
			_, _ = h.Write([]byte{0})
			_, _ = h.Write([]byte(labels[name]))
			_, _ = h.Write([]byte{0xff})
		}
	}
	return h.Sum64()
}
