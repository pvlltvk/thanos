// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Own label names never equal an external one; together they interleave in every way:
// "A" sorts before all own names, "zz" after all, the rest in between, in both cases.
var (
	resortOwnNames      = []string{"B", "Job", "Zone", "__name__", "_x", "a", "b", "job", "m", "x", "y", "zone", "zy"}
	resortExternalNames = []string{"A", "Cluster", "_e", "aa", "cluster", "n", "replica", "zone_ext", "zz"}
)

const resortAbsentReplicaLabel = "absent_replica"

type resortFragment struct {
	own      labels.Labels
	received []labelpb.ZLabel
	id       uint32
}

type resortInput struct {
	ext       labels.Labels
	remove    map[string]struct{}
	merged    bool
	fragments []resortFragment
}

func resortRandomInput(rng *rand.Rand) resortInput {
	for {
		if in := resortRandomInputOrEmpty(rng); len(in.fragments) > 0 {
			return in
		}
	}
}

func resortRandomInputOrEmpty(rng *rand.Rand) resortInput {
	in := resortInput{remove: map[string]struct{}{}}
	extBuilder := labels.NewBuilder(labels.EmptyLabels())
	for _, i := range rng.Perm(len(resortExternalNames))[:rng.Intn(4)] {
		extBuilder.Set(resortExternalNames[i], "e"+strconv.Itoa(rng.Intn(2)))
		if rng.Intn(3) == 0 {
			in.remove[resortExternalNames[i]] = struct{}{}
		}
	}
	in.ext = extBuilder.Labels()
	if rng.Intn(4) == 0 {
		in.remove[resortAbsentReplicaLabel] = struct{}{}
	}

	var names []string
	for _, name := range resortOwnNames {
		if rng.Intn(5) < 3 {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		names = resortOwnNames[:1]
	}
	seen := map[string]struct{}{}
	var sets []labels.Labels
	for range 1 + rng.Intn(40) {
		b := labels.NewBuilder(labels.EmptyLabels())
		first := 0
		if len(sets) > 0 && rng.Intn(2) == 0 {
			// Extend an earlier series so that it becomes a strict prefix of this one.
			parent := sets[rng.Intn(len(sets))]
			b.Reset(parent)
			last := ""
			parent.Range(func(l labels.Label) { last = l.Name })
			first = slices.Index(names, last) + 1
		}
		for _, name := range names[first:] {
			if rng.Intn(2) == 0 {
				b.Set(name, strconv.Itoa(rng.Intn(3)))
			}
		}
		set := b.Labels()
		if _, ok := seen[set.String()]; ok || set.IsEmpty() {
			continue
		}
		seen[set.String()] = struct{}{}
		sets = append(sets, set)
	}
	slices.SortFunc(sets, labels.Compare)

	// Prometheus merges its configured external labels into every frame; a sidecar with
	// externally supplied labels sees the series' own labels only.
	in.merged = rng.Intn(2) == 0
	for _, set := range sets {
		copies := 1
		if rng.Intn(5) == 0 {
			copies += 1 + rng.Intn(3)
		}
		for range copies {
			in.fragments = append(in.fragments, resortNewFragment(set, in.ext, in.merged, uint32(len(in.fragments))))
		}
	}
	return in
}

func resortNewFragment(own, ext labels.Labels, merged bool, id uint32) resortFragment {
	received := own
	if merged {
		b := labels.NewBuilder(ext)
		own.Range(func(l labels.Label) { b.Set(l.Name, l.Value) })
		received = b.Labels()
	}
	return resortFragment{own: own, received: labelpb.ZLabelsFromPromLabels(received), id: id}
}

type resortOutput struct {
	labels string
	id     uint32
}

func (in resortInput) final(builder *labels.Builder, f resortFragment) []labelpb.ZLabel {
	lset := labelpb.ZLabelsFromPromLabels(transformStoreLabels(builder, labelpb.ZLabelsToPromLabels(f.received), in.ext, in.remove))
	labelpb.ReAllocZLabelsStrings(&lset)
	return lset
}

func (in resortInput) fullSort() []resortOutput {
	var builder labels.Builder
	type record struct {
		lset labels.Labels
		id   uint32
	}
	records := make([]record, len(in.fragments))
	for i, f := range in.fragments {
		records[i] = record{lset: labelpb.ZLabelsToPromLabels(in.final(&builder, f)), id: f.id}
	}
	slices.SortStableFunc(records, func(a, b record) int { return labels.Compare(a.lset, b.lset) })
	out := make([]resortOutput, len(records))
	for i, r := range records {
		out[i] = resortOutput{labels: r.lset.String(), id: r.id}
	}
	return out
}

// streamed also returns the largest number of fragments and of distinct label sets held back.
func (in resortInput) streamed() (out []resortOutput, maxHeld, maxHeldSets int, err error) {
	srv := &seriesBufferTestServer{}
	s := newTestSeriesBuffer(srv, 0, false)
	var builder labels.Builder
	for _, f := range in.fragments {
		data := binary.BigEndian.AppendUint32(nil, f.id)
		if err = s.addSorted(f.received, in.final(&builder, f), []prompb.Chunk{{Type: prompb.Chunk_XOR, Data: data}}, in.ext, in.remove); err != nil {
			break
		}
		sets := 0
		for i := range s.series {
			if i == 0 || comparePrometheusSeries(s.series[i-1], s.series[i]) > 0 {
				sets++
			}
		}
		maxHeldSets = max(maxHeldSets, sets)
	}
	maxHeld = s.maxHeld
	if err == nil {
		err = s.Flush()
	}
	for _, response := range srv.responses {
		series := response.GetSeries()
		out = append(out, resortOutput{
			labels: labelpb.ZLabelsToPromLabels(series.Labels).String(),
			id:     binary.BigEndian.Uint32(series.Chunks[0].Raw.Data),
		})
	}
	return out, maxHeld, maxHeldSets, err
}

func (in resortInput) ownSorted() bool {
	return slices.IsSortedFunc(in.fragments, func(a, b resortFragment) int { return labels.Compare(a.own, b.own) })
}

func resortTrials(fallback int) int {
	if v := os.Getenv("THANOS_TEST_RESORT_TRIALS"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return fallback
}

func TestPrometheusSeriesBuffer_StreamingSortMatchesFullSort(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	trials := resortTrials(20000)
	var reordered, duplicates, fragments, maxHeld, maxHeldSets int
	heldSetsHistogram := map[int]int{}
	for trial := range trials {
		in := resortRandomInput(rng)
		want := in.fullSort()
		got, held, heldSets, err := in.streamed()
		if err != nil {
			t.Fatalf("trial %d: valid input rejected: %v\next=%s remove=%v", trial, err, in.ext, in.remove)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("trial %d: streamed order differs from full sort\next=%s remove=%v\n got=%v\nwant=%v", trial, in.ext, in.remove, got, want)
		}
		fragments += len(in.fragments)
		heldSetsHistogram[heldSets]++
		maxHeld = max(maxHeld, held)
		maxHeldSets = max(maxHeldSets, heldSets)
		for i, o := range want {
			if o.id != uint32(i) {
				reordered++
				break
			}
		}
		for i := 1; i < len(in.fragments); i++ {
			if labels.Equal(in.fragments[i-1].own, in.fragments[i].own) {
				duplicates++
				break
			}
		}
	}
	testutil.Assert(t, reordered > trials/20, "generator rarely needs reordering: %d of %d", reordered, trials)
	testutil.Assert(t, duplicates > trials/20, "generator rarely repeats label sets: %d of %d", duplicates, trials)
	t.Logf("trials=%d fragments=%d needed_reordering=%d with_repeated_labels=%d max_held_fragments=%d max_held_label_sets=%d held_label_sets_histogram=%v own_label_names=%d",
		trials, fragments, reordered, duplicates, maxHeld, maxHeldSets, heldSetsHistogram, len(resortOwnNames))
}

func TestPrometheusSeriesBuffer_StreamingSortRejectsViolations(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	trials := resortTrials(5000)

	assertRejectedOrSorted := func(t *testing.T, in resortInput, mustReject bool) (rejected bool) {
		t.Helper()
		got, _, _, err := in.streamed()
		if err == nil {
			if mustReject {
				t.Fatalf("violation accepted\next=%s remove=%v\nfragments=%v", in.ext, in.remove, in.fragments)
			}
			if want := in.fullSort(); !slices.Equal(got, want) {
				t.Fatalf("silently misordered\next=%s remove=%v\n got=%v\nwant=%v", in.ext, in.remove, got, want)
			}
			return false
		}
		testutil.Equals(t, codes.FailedPrecondition, status.Code(err))
		// Whatever was sent before the rejection must already be in its final position.
		want := in.fullSort()
		position := map[uint32]int{}
		for i, o := range want {
			position[o.id] = i
		}
		for i := 1; i < len(got); i++ {
			if position[got[i-1].id] > position[got[i].id] {
				t.Fatalf("misordered output before rejection\next=%s remove=%v\n got=%v\nwant=%v", in.ext, in.remove, got, want)
			}
		}
		return true
	}

	// The response stays sorted the way Prometheus sorts it: by all of the series' own labels.
	withOwnLabel := func(in resortInput, name, value string) resortInput {
		i := rng.Intn(len(in.fragments))
		own := labels.NewBuilder(in.fragments[i].own).Set(name, value).Labels()
		in.fragments = slices.Clone(in.fragments)
		in.fragments[i] = resortNewFragment(own, in.ext, in.merged, in.fragments[i].id)
		slices.SortStableFunc(in.fragments, func(a, b resortFragment) int { return labels.Compare(a.own, b.own) })
		return in
	}

	t.Run("unsorted response", func(t *testing.T) {
		var unsorted, rejected int
		for range trials {
			in := resortRandomInput(rng)
			rng.Shuffle(len(in.fragments), func(i, j int) { in.fragments[i], in.fragments[j] = in.fragments[j], in.fragments[i] })
			violation := !in.ownSorted()
			if violation {
				unsorted++
			}
			if assertRejectedOrSorted(t, in, violation) {
				rejected++
			}
		}
		testutil.Assert(t, unsorted > trials/2, "generator rarely breaks the order: %d of %d", unsorted, trials)
		t.Logf("trials=%d unsorted=%d rejected=%d", trials, unsorted, rejected)
	})

	t.Run("own value for external label", func(t *testing.T) {
		var tried, rejected int
		for range trials {
			in := resortRandomInput(rng)
			if in.ext.IsEmpty() {
				continue
			}
			var names []string
			in.ext.Range(func(l labels.Label) { names = append(names, l.Name) })
			tried++
			if assertRejectedOrSorted(t, withOwnLabel(in, names[rng.Intn(len(names))], "own"), true) {
				rejected++
			}
		}
		t.Logf("trials=%d rejected=%d", tried, rejected)
	})

	t.Run("own label named like removed replica label", func(t *testing.T) {
		var rejected int
		for range trials {
			in := resortRandomInput(rng)
			in.remove[resortAbsentReplicaLabel] = struct{}{}
			if assertRejectedOrSorted(t, withOwnLabel(in, resortAbsentReplicaLabel, "own"), true) {
				rejected++
			}
		}
		t.Logf("trials=%d rejected=%d", trials, rejected)
	})

	// Indistinguishable from a label Prometheus merged in, so it cannot always be
	// rejected; it must then still come out in full sort order.
	t.Run("own label equal to external label", func(t *testing.T) {
		var tried, rejected int
		for range trials {
			in := resortRandomInput(rng)
			if in.ext.IsEmpty() {
				continue
			}
			var external []labels.Label
			in.ext.Range(func(l labels.Label) { external = append(external, l) })
			l := external[rng.Intn(len(external))]
			tried++
			if assertRejectedOrSorted(t, withOwnLabel(in, l.Name, l.Value), false) {
				rejected++
			}
		}
		t.Logf("trials=%d rejected=%d accepted_in_full_sort_order=%d", tried, rejected, tried-rejected)
	})
}

type resortRecordingServer struct {
	storepb.Store_SeriesServer
	ctx       context.Context
	responses []*storepb.SeriesResponse
	onSend    func(sends int) error
}

func (s *resortRecordingServer) Context() context.Context { return s.ctx }

func (s *resortRecordingServer) Send(r *storepb.SeriesResponse) error {
	if s.onSend != nil {
		if err := s.onSend(len(s.responses) + 1); err != nil {
			return err
		}
	}
	s.responses = append(s.responses, r)
	return nil
}

type resortResponse struct {
	batch  bool
	series []resortSeries
}

type resortSeries struct {
	labels string
	chunks []storepb.AggrChunk
}

func (s *resortRecordingServer) summary() []resortResponse {
	out := make([]resortResponse, 0, len(s.responses))
	for _, response := range s.responses {
		r := resortResponse{batch: response.GetBatch() != nil}
		series := []*storepb.Series{response.GetSeries()}
		if r.batch {
			series = response.GetBatch().Series
		}
		for _, item := range series {
			r.series = append(r.series, resortSeries{labels: labelpb.ZLabelsToPromLabels(item.Labels).String(), chunks: item.Chunks})
		}
		out = append(out, r)
	}
	return out
}

func (s *resortRecordingServer) seriesCount() (n int) {
	for _, r := range s.summary() {
		n += len(r.series)
	}
	return n
}

func resortFrames(t *testing.T, in resortInput) [][]byte {
	t.Helper()
	frames := make([][]byte, 0, len(in.fragments))
	for _, f := range in.fragments {
		series := &prompb.ChunkedSeries{Labels: f.received}
		for j := range 1 + int(f.id%3) {
			chunk := chunkenc.NewXORChunk()
			appender, err := chunk.Appender()
			testutil.Ok(t, err)
			for k := range 5 {
				appender.Append(int64(j*1000+k), float64(f.id)+float64(k)/8)
			}
			series.Chunks = append(series.Chunks, prompb.Chunk{MinTimeMs: int64(j * 1000), MaxTimeMs: int64(j*1000 + 4), Type: prompb.Chunk_XOR, Data: chunk.Bytes()})
		}
		frames = append(frames, chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{series}}))
	}
	return frames
}

func resortStore(t *testing.T, reg prometheus.Registerer, ext labels.Labels, body *trackedFramesBody, options ...PrometheusStoreOption) *PrometheusStore {
	t.Helper()
	client := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{streamedProtoContentType}}, Body: body}, nil
	})
	store, err := newResortStore(reg, client, ext, options...)
	testutil.Ok(t, err)
	return store
}

func newResortStore(reg prometheus.Registerer, client promclient.HTTPClient, ext labels.Labels, options ...PrometheusStoreOption) (*PrometheusStore, error) {
	return NewPrometheusStore(nil, reg, promclient.NewClient(client, nil, ""), &url.URL{Scheme: "http", Host: "prometheus.test"}, component.Sidecar,
		func() labels.Labels { return ext }, func() (int64, int64) { return 0, -1 }, nil, options...)
}

// resortFixture is a fixed response in which a full sort moves series, label sets repeat
// in consecutive frames and nested prefixes are held back.
func resortFixture(t *testing.T, merged bool) resortInput {
	t.Helper()
	ext := labels.FromStrings("A", "first", "cluster", "c1", "replica", "r1", "zone_ext", "last")
	own := []labels.Labels{
		labels.FromStrings("__name__", "up"),
		labels.FromStrings("__name__", "up", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "a", "zone", "x"),
		labels.FromStrings("__name__", "up", "job", "a", "zone", "x"),
		labels.FromStrings("__name__", "up", "job", "a", "zone", "x", "zy", "1"),
		labels.FromStrings("__name__", "up", "job", "a", "zone", "y"),
		labels.FromStrings("__name__", "up", "job", "a", "zy", "1"),
		labels.FromStrings("__name__", "up", "job", "b"),
		labels.FromStrings("__name__", "up", "zone", "x"),
	}
	for i := range 150 {
		base := []string{"__name__", fmt.Sprintf("metric_%03d", i/3), "Job", strconv.Itoa(i % 3)}
		own = append(own, labels.FromStrings(base...))
		if i%2 == 0 {
			own = append(own, labels.FromStrings(append(base, "instance", "i1")...), labels.FromStrings(append(base, "instance", "i1", "le", "1")...))
		}
	}
	slices.SortFunc(own, labels.Compare)
	in := resortInput{ext: ext, remove: map[string]struct{}{}}
	for i, set := range own {
		in.fragments = append(in.fragments, resortNewFragment(set, ext, merged, uint32(i)))
	}
	testutil.Assert(t, in.ownSorted())
	want := in.fullSort()
	testutil.Assert(t, !slices.IsSortedFunc(want, func(a, b resortOutput) int { return int(a.id) - int(b.id) }), "fixture needs no reordering")
	return in
}

func TestPrometheusStore_Series_StreamingSortEqualsBuffered(t *testing.T) {
	for _, merged := range []bool{false, true} {
		in := resortFixture(t, merged)
		frames := resortFrames(t, in)

		run := func(t *testing.T, r *storepb.SeriesRequest, limits *SeriesSelectLimits, srv *resortRecordingServer, options ...PrometheusStoreOption) (*trackedFramesBody, *prometheus.Registry, error) {
			t.Helper()
			reg := prometheus.NewRegistry()
			if limits != nil {
				options = append(options, WithPrometheusStoreLimits(*limits))
			}
			body := newTrackedFramesBody(frames)
			request := *r
			err := resortStore(t, reg, in.ext, body, options...).Series(&request, srv)
			testutil.Assert(t, body.isClosed(), "response body was not closed")
			return body, reg, err
		}
		request := func(batchSize int64, shard *storepb.ShardInfo, withoutReplica []string) *storepb.SeriesRequest {
			return &storepb.SeriesRequest{
				MinTime: 0, MaxTime: 1 << 40,
				Matchers:             []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}},
				ResponseBatchSize:    batchSize,
				ShardInfo:            shard,
				WithoutReplicaLabels: withoutReplica,
			}
		}
		streaming := WithPrometheusStoreStreamingSort(true)

		for _, batchSize := range []int64{0, 1, 64} {
			for _, limits := range []*SeriesSelectLimits{nil, {SeriesPerRequest: 100000, SamplesPerRequest: 100000000}} {
				for shardName, shard := range map[string]*storepb.ShardInfo{
					"unsharded":       nil,
					"shard by":        {ShardIndex: 1, TotalShards: 3, By: true, Labels: []string{"__name__", "zone"}},
					"shard without":   {ShardIndex: 0, TotalShards: 2, By: false, Labels: []string{"Job", "cluster"}},
					"shard by ext":    {ShardIndex: 0, TotalShards: 2, By: true, Labels: []string{"replica", "le"}},
					"shard by absent": {ShardIndex: 1, TotalShards: 2, By: true, Labels: []string{"nope"}},
				} {
					for _, withoutReplica := range [][]string{nil, {"replica"}, {"zone_ext", "replica", "unknown"}} {
						name := fmt.Sprintf("merged=%t/batch=%d/limits=%t/%s/without=%v", merged, batchSize, limits != nil, shardName, withoutReplica)
						t.Run(name, func(t *testing.T) {
							buffered := &resortRecordingServer{ctx: context.Background()}
							_, _, err := run(t, request(batchSize, shard, withoutReplica), limits, buffered)
							testutil.Ok(t, err)
							streamed := &resortRecordingServer{ctx: context.Background()}
							_, _, err = run(t, request(batchSize, shard, withoutReplica), limits, streamed, streaming)
							testutil.Ok(t, err)
							testutil.Equals(t, buffered.summary(), streamed.summary())
							if shard == nil {
								testutil.Equals(t, len(in.fragments), streamed.seriesCount())
							} else if shardName != "shard by absent" {
								testutil.Assert(t, streamed.seriesCount() > 0 && streamed.seriesCount() < len(in.fragments), "shard selects %d of %d series", streamed.seriesCount(), len(in.fragments))
							}
						})
					}
				}
			}
		}

		t.Run(fmt.Sprintf("merged=%t/disk sorting is not used by streamed responses", merged), func(t *testing.T) {
			dir := t.TempDir()
			buffered := &resortRecordingServer{ctx: context.Background()}
			_, _, err := run(t, request(64, nil, []string{"replica"}), nil, buffered)
			testutil.Ok(t, err)
			streamed := &resortRecordingServer{ctx: context.Background(), onSend: func(int) error {
				assertSpillDirectoryEmpty(t, dir)
				return nil
			}}
			_, _, err = run(t, request(64, nil, []string{"replica"}), nil, streamed, streaming, WithPrometheusStoreSorting(1, dir, 0))
			testutil.Ok(t, err)
			testutil.Equals(t, buffered.summary(), streamed.summary())
			assertSpillDirectoryEmpty(t, dir)
		})

		t.Run(fmt.Sprintf("merged=%t/series limit", merged), func(t *testing.T) {
			limits := &SeriesSelectLimits{SeriesPerRequest: 40}
			buffered := &resortRecordingServer{ctx: context.Background()}
			_, wantReg, wantErr := run(t, request(1, nil, nil), limits, buffered)
			testutil.NotOk(t, wantErr)
			streamed := &resortRecordingServer{ctx: context.Background()}
			_, gotReg, gotErr := run(t, request(1, nil, nil), limits, streamed, streaming)
			testutil.NotOk(t, gotErr)
			testutil.Equals(t, wantErr.Error(), gotErr.Error())
			testutil.Equals(t, status.Code(wantErr), status.Code(gotErr))
			testutil.Equals(t, selectDropCount(t, wantReg, "series"), selectDropCount(t, gotReg, "series"))
			testutil.Equals(t, 0, buffered.seriesCount())
			// Series sent before the limit was hit are a prefix of the full result.
			full := &resortRecordingServer{ctx: context.Background()}
			_, _, err := run(t, request(1, nil, nil), nil, full)
			testutil.Ok(t, err)
			testutil.Assert(t, streamed.seriesCount() <= 40)
			testutil.Equals(t, full.summary()[:streamed.seriesCount()], streamed.summary())
		})

		for _, batchSize := range []int64{1, 64} {
			sendErr := errors.New("client went away")
			failAt := 2
			if batchSize == 1 {
				failAt = len(in.fragments) / 2
			}
			t.Run(fmt.Sprintf("merged=%t/batch=%d/send error", merged, batchSize), func(t *testing.T) {
				fail := func(sends int) error {
					if sends == failAt {
						return sendErr
					}
					return nil
				}
				buffered := &resortRecordingServer{ctx: context.Background(), onSend: fail}
				wantBody, _, wantErr := run(t, request(batchSize, nil, nil), nil, buffered)
				streamed := &resortRecordingServer{ctx: context.Background(), onSend: fail}
				gotBody, _, gotErr := run(t, request(batchSize, nil, nil), nil, streamed, streaming)
				testutil.Equals(t, sendErr, wantErr)
				testutil.Equals(t, sendErr, gotErr)
				testutil.Equals(t, buffered.summary(), streamed.summary())
				testutil.Equals(t, len(frames), wantBody.framesConsumed())
				testutil.Assert(t, gotBody.framesConsumed() < len(frames), "streaming kept reading after the client failed: %d of %d frames", gotBody.framesConsumed(), len(frames))
			})

			t.Run(fmt.Sprintf("merged=%t/batch=%d/context canceled", merged, batchSize), func(t *testing.T) {
				run := func(options ...PrometheusStoreOption) (*trackedFramesBody, *resortRecordingServer, error) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					srv := &resortRecordingServer{ctx: ctx}
					srv.onSend = func(sends int) error {
						if sends == failAt {
							cancel()
						}
						return nil
					}
					body, _, err := run(t, request(batchSize, nil, nil), nil, srv, options...)
					return body, srv, err
				}
				_, buffered, wantErr := run()
				gotBody, streamed, gotErr := run(streaming)
				testutil.Equals(t, context.Canceled, wantErr)
				testutil.Equals(t, context.Canceled, gotErr)
				testutil.Equals(t, buffered.summary(), streamed.summary())
				testutil.Assert(t, gotBody.framesConsumed() < len(frames), "streaming kept reading after cancellation: %d of %d frames", gotBody.framesConsumed(), len(frames))
			})
		}
	}
}

func TestPrometheusStore_Series_StreamingSortRejections(t *testing.T) {
	ext := labels.FromStrings("cluster", "c1", "replica", "r1")
	request := &storepb.SeriesRequest{
		MinTime: 0, MaxTime: 1 << 40,
		Matchers:             []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}},
		WithoutReplicaLabels: []string{"replica", "pod"},
	}
	series := func(lset ...string) *prompb.ChunkedSeries {
		return prometheusChunkedSeriesWithLabels(t, labelpb.ZLabelsFromPromLabels(labels.FromStrings(lset...)), 1)
	}
	for _, tc := range []struct {
		name   string
		series []*prompb.ChunkedSeries
		want   string
		sent   int
	}{
		{
			name:   "unsorted",
			series: []*prompb.ChunkedSeries{series("__name__", "a"), series("__name__", "c"), series("__name__", "b")},
			want:   `rpc error: code = FailedPrecondition desc = streaming series sort cannot be used for this response: response is not sorted by the series' own labels, {__name__="b"} follows {__name__="c"}`,
			sent:   1,
		},
		{
			name:   "prefix after extension",
			series: []*prompb.ChunkedSeries{series("__name__", "a", "job", "x"), series("__name__", "a")},
			want:   `rpc error: code = FailedPrecondition desc = streaming series sort cannot be used for this response: response is not sorted by the series' own labels, {__name__="a"} follows {__name__="a", job="x"}`,
		},
		{
			name:   "own value for external label",
			series: []*prompb.ChunkedSeries{series("__name__", "a"), series("__name__", "b", "cluster", "own")},
			want:   `rpc error: code = FailedPrecondition desc = streaming series sort cannot be used for this response: series {__name__="b", cluster="own"} has its own value for external label "cluster"`,
		},
		{
			name:   "own label is a removed replica label",
			series: []*prompb.ChunkedSeries{series("__name__", "a", "pod", "p")},
			want:   `rpc error: code = FailedPrecondition desc = streaming series sort cannot be used for this response: series {__name__="a", pod="p"} has removed replica label "pod" as its own label`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var frames [][]byte
			for _, s := range tc.series {
				frames = append(frames, chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{s}}))
			}
			buffered := &resortRecordingServer{ctx: context.Background()}
			r := *request
			testutil.Ok(t, resortStore(t, nil, ext, newTrackedFramesBody(frames)).Series(&r, buffered))
			testutil.Equals(t, len(tc.series), buffered.seriesCount())

			body := newTrackedFramesBody(frames)
			streamed := &resortRecordingServer{ctx: context.Background()}
			r = *request
			err := resortStore(t, nil, ext, body, WithPrometheusStoreStreamingSort(true)).Series(&r, streamed)
			testutil.NotOk(t, err)
			testutil.Equals(t, tc.want, err.Error())
			testutil.Equals(t, codes.FailedPrecondition, status.Code(err))
			testutil.Equals(t, tc.sent, streamed.seriesCount())
			testutil.Assert(t, body.isClosed(), "response body was not closed")
		})
	}
}

func resortUnstreamable(t *testing.T, store *PrometheusStore) map[string]float64 {
	t.Helper()
	testutil.Equals(t, 3, promtest.CollectAndCount(store.seriesSortUnstreamable))
	counts := map[string]float64{}
	for _, reason := range []string{streamingSortExternalLabel, streamingSortReplicaLabel, streamingSortUnsorted} {
		if n := promtest.ToFloat64(store.seriesSortUnstreamable.WithLabelValues(reason)); n != 0 {
			counts[reason] = n
		}
	}
	return counts
}

func TestPrometheusStore_Series_StreamingSortUnstreamable(t *testing.T) {
	ext := labels.FromStrings("cluster", "c1", "replica", "r1")
	remove := map[string]struct{}{"replica": {}, "pod": {}}
	request := &storepb.SeriesRequest{
		MinTime: 0, MaxTime: 1 << 40,
		Matchers:             []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}},
		WithoutReplicaLabels: []string{"replica", "pod"},
	}
	l := labels.FromStrings
	errString := func(err error) string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	for _, tc := range []struct {
		name   string
		series []labels.Labels
		limit  uint64
		reason string
	}{
		{
			// {__name__="a", b="1"} sorts before {__name__="a"} once cluster is added.
			name:   "compatible",
			series: []labels.Labels{l("__name__", "a"), l("__name__", "a", "b", "1"), l("__name__", "b", "cluster", "c1"), l("__name__", "c")},
		},
		{
			name:   "own values for an external label",
			series: []labels.Labels{l("__name__", "a", "b", "1", "cluster", "own"), l("__name__", "a", "cluster", "own"), l("__name__", "b"), l("__name__", "c", "cluster", "other")},
			reason: "external_label",
		},
		{
			name:   "own labels named like a removed replica label",
			series: []labels.Labels{l("__name__", "a", "pod", "p"), l("__name__", "b", "pod", "p"), l("__name__", "b", "pod", "q")},
			reason: "replica_label",
		},
		{
			name:   "unsorted",
			series: []labels.Labels{l("__name__", "d"), l("__name__", "c"), l("__name__", "b"), l("__name__", "a")},
			reason: "unsorted",
		},
		{
			name:   "first violation decides the reason",
			series: []labels.Labels{l("__name__", "b"), l("__name__", "a"), l("__name__", "c", "cluster", "own"), l("__name__", "d", "pod", "p")},
			reason: "unsorted",
		},
		{
			name:   "violation before the series limit",
			series: []labels.Labels{l("__name__", "a", "cluster", "own"), l("__name__", "b"), l("__name__", "c")},
			limit:  2,
			reason: "external_label",
		},
		{
			name:   "series limit before the violation",
			series: []labels.Labels{l("__name__", "a"), l("__name__", "b"), l("__name__", "c", "cluster", "own")},
			limit:  2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var options []PrometheusStoreOption
			if tc.limit > 0 {
				options = append(options, WithPrometheusStoreLimits(SeriesSelectLimits{SeriesPerRequest: tc.limit}))
			}
			want := map[string]float64{}
			if tc.reason != "" {
				want[tc.reason] = 1
			}

			// The buffer on its own is the buffered path without the dry run.
			baseline := &resortRecordingServer{ctx: context.Background()}
			buffer := resortStore(t, nil, ext, nil, options...).newSeriesBuffer(baseline, 0, false)
			var (
				frames  [][]byte
				builder labels.Builder
				wantErr error
			)
			for i, lset := range tc.series {
				series := prometheusChunkedSeriesWithLabels(t, labelpb.ZLabelsFromPromLabels(lset), float64(i))
				frames = append(frames, chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{series}}))
				if wantErr == nil {
					wantErr = buffer.add(labelpb.ZLabelsFromPromLabels(transformStoreLabels(&builder, lset, ext, remove)), series.Chunks)
				}
			}
			if wantErr == nil {
				wantErr = buffer.Flush()
			}
			testutil.Ok(t, buffer.Close())
			testutil.Equals(t, tc.limit > 0, wantErr != nil)

			buffered := resortStore(t, nil, ext, newTrackedFramesBody(frames), options...)
			got := &resortRecordingServer{ctx: context.Background()}
			r := *request
			testutil.Equals(t, errString(wantErr), errString(buffered.Series(&r, got)))
			testutil.Equals(t, baseline.summary(), got.summary())
			testutil.Equals(t, want, resortUnstreamable(t, buffered))

			streaming := resortStore(t, nil, ext, newTrackedFramesBody(frames), append(options, WithPrometheusStoreStreamingSort(true))...)
			got = &resortRecordingServer{ctx: context.Background()}
			r = *request
			err := streaming.Series(&r, got)
			switch {
			case tc.reason != "":
				testutil.Equals(t, codes.FailedPrecondition, status.Code(err))
			case wantErr != nil:
				testutil.Equals(t, wantErr.Error(), err.Error())
			default:
				testutil.Ok(t, err)
				testutil.Equals(t, baseline.summary(), got.summary())
			}
			testutil.Equals(t, want, resortUnstreamable(t, streaming))
		})
	}
}

func TestPrometheusStore_Series_StreamingSortDryRunWarning(t *testing.T) {
	series := prometheusChunkedSeriesWithLabels(t, labelpb.ZLabelsFromPromLabels(labels.FromStrings("__name__", "a", "cluster", "own")), 1)
	frame := chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{series}})
	client := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{streamedProtoContentType}}, Body: newTrackedFramesBody([][]byte{frame})}, nil
	})
	store, err := newResortStore(nil, client, labels.FromStrings("cluster", "c1"))
	testutil.Ok(t, err)
	var logs bytes.Buffer
	store.logger = log.NewLogfmtLogger(&logs)
	warnings := func(requests int) int {
		for range requests {
			r := &storepb.SeriesRequest{MinTime: 0, MaxTime: 1 << 40, Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "a"}}}
			testutil.Ok(t, store.Series(r, &resortRecordingServer{ctx: context.Background()}))
		}
		return strings.Count(logs.String(), "level=warn")
	}

	testutil.Equals(t, 1, warnings(3))
	testutil.Assert(t, strings.Contains(logs.String(), `reason=external_label lset="{__name__=\"a\", cluster=\"own\"}"`), "unexpected log: %s", logs.String())
	store.seriesSortUnstreamableWarnedAt.Sub(int64(streamingSortWarnInterval))
	testutil.Equals(t, 2, warnings(2))
	testutil.Equals(t, map[string]float64{"external_label": 5}, resortUnstreamable(t, store))
}
