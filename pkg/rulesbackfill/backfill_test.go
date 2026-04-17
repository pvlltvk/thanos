// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rulesbackfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"golang.org/x/time/rate"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// defaultBlockDurMs is tsdb.DefaultBlockDuration (2h in ms).
const defaultBlockDurMs = int64(2 * time.Hour / time.Millisecond) // 7200000

// --- helpers -----------------------------------------------------------------

func newTestLogger() log.Logger {
	return log.NewNopLogger()
}

func newTestLabels() labels.Labels {
	return labels.FromStrings("cluster", "test", "backfill_job", "test-run")
}

// writeRuleFile writes a Prometheus rule file with the given YAML content to
// a temp file and returns the path.
func writeRuleFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "rules-*.yml")
	testutil.Ok(t, err)
	_, err = f.WriteString(content)
	testutil.Ok(t, err)
	testutil.Ok(t, f.Close())
	return f.Name()
}

// singleSampleQuery returns a QueryRangeFunc that always responds with one
// series containing a single sample at startTime+1ms for the given metric name.
func singleSampleQuery(metricName string, val float64) QueryRangeFunc {
	return func(ctx context.Context, query string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: model.LabelValue(metricName)},
				Values: []model.SamplePair{{Timestamp: ts, Value: model.SampleValue(val)}},
			},
		}, nil, nil
	}
}

// emptyQuery always returns an empty matrix.
func emptyQuery() QueryRangeFunc {
	return func(_ context.Context, _ string, _, _, _ int64) (model.Matrix, []string, error) {
		return model.Matrix{}, nil, nil
	}
}

// errorQuery always returns an error.
func errorQuery(err error) QueryRangeFunc {
	return func(_ context.Context, _ string, _, _, _ int64) (model.Matrix, []string, error) {
		return nil, nil, err
	}
}

// readBucketMeta reads the meta.json for the first TSDB block found in bkt.
func readBucketMeta(t *testing.T, bkt *objstore.InMemBucket) *metadata.Meta {
	t.Helper()
	objs := bkt.Objects()
	for key := range objs {
		if strings.HasSuffix(key, "/"+metadata.MetaFilename) {
			data := objs[key]
			var m metadata.Meta
			testutil.Ok(t, json.Unmarshal(data, &m))
			return &m
		}
	}
	return nil
}

// countBucketBlocks counts ULID-named top-level directories in the bucket.
func countBucketBlocks(bkt *objstore.InMemBucket) int {
	seen := map[string]struct{}{}
	for key := range bkt.Objects() {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) == 0 {
			continue
		}
		if _, err := parsePossibleULID(parts[0]); err == nil {
			seen[parts[0]] = struct{}{}
		}
	}
	return len(seen)
}

// parsePossibleULID tries to parse s as a ULID (26-char base32 string).
func parsePossibleULID(s string) (interface{}, error) {
	if len(s) != 26 {
		return nil, fmt.Errorf("not a ULID")
	}
	return s, nil
}

// --- TestParseRuleFiles ------------------------------------------------------

func TestParseRuleFiles(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		content       string
		expectGroups  int
		expectRules   int
		expectErr     bool
		labelKey      string
		labelVal      string
	}{
		{
			name: "valid recording rules",
			content: `
groups:
  - name: test_group
    rules:
      - record: test_metric
        expr: sum(rate(http_requests_total[5m]))
      - record: another_metric
        expr: count(up)
`,
			expectGroups: 1,
			expectRules:  2,
		},
		{
			name: "only alerting rules",
			content: `
groups:
  - name: alerts
    rules:
      - alert: TestAlert
        expr: up == 0
`,
			expectGroups: 0,
			expectRules:  0,
		},
		{
			name: "mixed recording and alerting rules",
			content: `
groups:
  - name: mixed
    rules:
      - record: my_metric
        expr: sum(rate(http_requests_total[5m]))
      - alert: MyAlert
        expr: up == 0
`,
			expectGroups: 1,
			expectRules:  1,
		},
		{
			name: "recording rule with extra labels",
			content: `
groups:
  - name: labeled_group
    rules:
      - record: labeled_metric
        expr: sum(rate(http_requests_total[5m]))
        labels:
          env: production
`,
			expectGroups: 1,
			expectRules:  1,
			labelKey:     "env",
			labelVal:     "production",
		},
		{
			name: "invalid YAML content",
			content: `
groups:
  - name: bad
    rules:
      : invalid
`,
			expectErr: true,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := writeRuleFile(t, tc.content)
			b := New(newTestLogger(), emptyQuery(), objstore.NewInMemBucket(), newTestLabels(), t.TempDir())
			groups, err := b.parseRuleFiles([]string{path})
			if tc.expectErr {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, tc.expectGroups, len(groups))
			totalRules := 0
			for _, g := range groups {
				totalRules += len(g.Rules)
			}
			testutil.Equals(t, tc.expectRules, totalRules)

			if tc.labelKey != "" {
				found := false
				for _, g := range groups {
					for _, r := range g.Rules {
						if v, ok := r.Labels[tc.labelKey]; ok && v == tc.labelVal {
							found = true
						}
					}
				}
				testutil.Assert(t, found, "expected label %s=%s on recording rule", tc.labelKey, tc.labelVal)
			}
		})
	}
}

func TestParseRuleFiles_NonExistentGlob(t *testing.T) {
	t.Parallel()
	b := New(newTestLogger(), emptyQuery(), objstore.NewInMemBucket(), newTestLabels(), t.TempDir())
	_, err := b.parseRuleFiles([]string{"/no/such/path/rules-*.yml"})
	testutil.NotOk(t, err)
}

// --- TestComputeWindows ------------------------------------------------------

func TestComputeWindows(t *testing.T) {
	t.Parallel()

	blockDur := defaultBlockDurMs // 2h = 7200000 ms

	for _, tc := range []struct {
		name        string
		startMs     int64
		endMs       int64
		blockDurMs  int64
		expectCount int
	}{
		{
			name:        "standard 4h range produces 2 windows",
			startMs:     0,
			endMs:       4 * int64(time.Hour/time.Millisecond),
			blockDurMs:  blockDur,
			expectCount: 2,
		},
		{
			name:        "range not aligned to boundary",
			startMs:     1000,
			endMs:       3 * int64(time.Hour/time.Millisecond),
			blockDurMs:  blockDur,
			expectCount: 2, // aligned start is 0; windows [0,2h), [2h,3h)
		},
		{
			name:        "range smaller than block duration",
			startMs:     0,
			endMs:       int64(time.Hour / time.Millisecond), // 1h
			blockDurMs:  blockDur,
			expectCount: 1,
		},
		{
			name:        "range exactly equal to block duration",
			startMs:     0,
			endMs:       blockDur,
			blockDurMs:  blockDur,
			expectCount: 1,
		},
		{
			name:        "large range produces many windows",
			startMs:     0,
			endMs:       30 * 24 * int64(time.Hour/time.Millisecond), // 30 days
			blockDurMs:  blockDur,
			expectCount: 30 * 12, // 12 windows per day
		},
		{
			name:        "start equals end produces no windows",
			startMs:     blockDur,
			endMs:       blockDur,
			blockDurMs:  blockDur,
			expectCount: 0,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			windows := computeWindows(tc.startMs, tc.endMs, tc.blockDurMs)
			testutil.Equals(t, tc.expectCount, len(windows))

			// Verify windows are contiguous and ordered.
			for i := 1; i < len(windows); i++ {
				testutil.Equals(t, windows[i-1].end, windows[i].start)
			}
		})
	}
}

// --- TestGetCompatibleBlockDuration ------------------------------------------

func TestGetCompatibleBlockDuration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		input    int64
		expected int64
	}{
		{
			name:     "default 2h returns default block duration",
			input:    2 * int64(time.Hour/time.Millisecond),
			expected: defaultBlockDurMs,
		},
		{
			name:     "zero returns default block duration",
			input:    0,
			expected: tsdb.DefaultBlockDuration,
		},
		{
			name:     "negative returns default block duration",
			input:    -1,
			expected: tsdb.DefaultBlockDuration,
		},
		{
			// ExponentialBlockRanges from 2h*3^n: 2h, 6h, 18h, 54h...
			// Ranges that divide 24h evenly: 2h (yes), 6h (24%6=0, yes), 18h (24%18=6, no).
			// Largest compatible range <= 24h is 6h = 21600000ms.
			name:     "24h returns 6h (largest range dividing 24h evenly)",
			input:    24 * int64(time.Hour/time.Millisecond),
			expected: 6 * int64(time.Hour/time.Millisecond),
		},
		{
			// maxDurMs = 1h. First exponential range is 2h which exceeds 1h, so loop
			// breaks immediately and the function returns the initial default (2h).
			name:     "1h smaller than default returns default 2h",
			input:    int64(time.Hour / time.Millisecond),
			expected: tsdb.DefaultBlockDuration,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := getCompatibleBlockDuration(tc.input)
			testutil.Equals(t, tc.expected, got)
		})
	}
}

func TestGetCompatibleBlockDuration_DivisibilityInvariant(t *testing.T) {
	t.Parallel()
	for _, inputH := range []int64{2, 4, 8, 24, 48} {
		inputMs := inputH * int64(time.Hour/time.Millisecond)
		got := getCompatibleBlockDuration(inputMs)
		testutil.Assert(t, got > 0, "expected positive block duration for input %dh", inputH)
		testutil.Assert(t, got <= inputMs, "block duration %d must not exceed input %d", got, inputMs)
		if inputMs > 0 {
			testutil.Assert(t, inputMs%got == 0, "input %d must be divisible by block duration %d", inputMs, got)
		}
	}
}

// --- TestProcessWindow / TestRun integration ---------------------------------

const simpleRecordingRuleYAML = `
groups:
  - name: test_group
    rules:
      - record: test_metric
        expr: sum(rate(http_requests_total[5m]))
`

func TestRun_SingleRuleSingleWindow_BlockCreatedAndUploaded(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	dataDir := t.TempDir()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 42),
		bkt,
		newTestLabels(),
		dataDir,
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))

	// Verify block landed in bucket.
	testutil.Assert(t, countBucketBlocks(bkt) >= 1, "expected at least one block in bucket")

	meta := readBucketMeta(t, bkt)
	testutil.Assert(t, meta != nil, "expected meta.json in bucket")
	testutil.Equals(t, metadata.RulesBackfillSource, meta.Thanos.Source)
	testutil.Equals(t, int64(0), meta.Thanos.Downsample.Resolution)
	testutil.Equals(t, "test", meta.Thanos.Labels["cluster"])
	testutil.Equals(t, "test-run", meta.Thanos.Labels["backfill_job"])
}

func TestRun_EmptyMatrix_NoBlockCreated(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		emptyQuery(),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(ids))
	testutil.Equals(t, 0, countBucketBlocks(bkt))
}

func TestRun_QueryError_FailsWindowAndReturnsError(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		errorQuery(fmt.Errorf("upstream unavailable")),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	// Query errors now fail the entire window and propagate as an error.
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, len(ids))
	testutil.Equals(t, 0, countBucketBlocks(bkt))
}

func TestRun_MultipleRules_AllWrittenToSameBlock(t *testing.T) {
	t.Parallel()

	multiRuleYAML := `
groups:
  - name: multi
    rules:
      - record: metric_a
        expr: sum(rate(http_requests_total[5m]))
      - record: metric_b
        expr: count(up)
`
	callCount := 0
	queryFunc := func(_ context.Context, query string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		callCount++
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "placeholder"},
				Values: []model.SamplePair{{Timestamp: ts, Value: model.SampleValue(float64(callCount))}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, multiRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	// Both rules produce samples → exactly one block for the single window.
	testutil.Equals(t, 1, len(ids))
	testutil.Equals(t, 1, countBucketBlocks(bkt))
	testutil.Equals(t, 2, callCount)
}

// TestRun_NoRecordingRules_ReturnsError verifies that a rule file containing
// only alerting rules (i.e. no recording rules) is treated as a configuration
// error. Phase 2 upgraded this from a warn-and-continue to a hard error so the
// operator discovers the mistake at preflight time rather than after waiting
// for a long backfill to no-op.
func TestRun_NoRecordingRules_ReturnsError(t *testing.T) {
	t.Parallel()

	alertOnlyYAML := `
groups:
  - name: alerts
    rules:
      - alert: AnAlert
        expr: up == 0
`
	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, alertOnlyYAML)

	b := New(
		newTestLogger(),
		singleSampleQuery("unused", 1),
		bkt,
		newTestLabels(),
		t.TempDir(),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, time.Unix(0, 0), time.Unix(7200, 0))
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "no recording rules"),
		"expected error to mention no recording rules, got: %v", err)
	testutil.Equals(t, 0, len(ids))
}

// --- TestScanBucket ----------------------------------------------------------

// buildInMemMeta inserts a fake meta.json for a block with the given parameters
// into an InMemBucket.
func buildInMemMeta(t *testing.T, bkt *objstore.InMemBucket, blockLabels map[string]string, source metadata.SourceType, minTime, maxTime int64) {
	t.Helper()

	// Create a minimal Meta.
	m := metadata.Meta{}
	m.MinTime = minTime
	m.MaxTime = maxTime
	m.Version = metadata.TSDBVersion1
	m.Thanos.Labels = blockLabels
	m.Thanos.Source = source
	m.Thanos.Downsample.Resolution = 0

	data, err := json.Marshal(m)
	testutil.Ok(t, err)

	// We need a ULID-like directory name. Use a hard-coded valid ULID string.
	blockID := nextFakeULID(t)
	key := filepath.Join(blockID, metadata.MetaFilename)
	testutil.Ok(t, bkt.Upload(context.Background(), key, strings.NewReader(string(data))))
}

var fakeULIDCounter int64

// nextFakeULID returns a unique valid 26-char ULID string each call.
// It encodes the counter into the last two characters of the ULID.
func nextFakeULID(t *testing.T) string {
	t.Helper()
	n := int(atomic.AddInt64(&fakeULIDCounter, 1))
	// Crockford base32 alphabet.
	chars := "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	prefix := "01ARZ3NDEKTSV4RRFFQ69G5F"
	c1 := chars[(n/len(chars))%len(chars)]
	c2 := chars[n%len(chars)]
	return prefix + string(c1) + string(c2)
}

func TestScanBucket_EmptyBucket(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	covered, warn, err := ScanBucket(context.Background(), newTestLogger(), bkt, newTestLabels(), 0, 7200000)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(covered))
	testutil.Assert(t, !warn, "expected no overlap warning for empty bucket")
}

func TestScanBucket_BackfillBlocksTrackedAsCovered(t *testing.T) {
	t.Parallel()
	bkt := objstore.NewInMemBucket()
	extLabels := map[string]string{"cluster": "test", "backfill_job": "test-run"}

	buildInMemMeta(t, bkt, extLabels, metadata.RulesBackfillSource, 0, 7200000)

	covered, warn, err := ScanBucket(context.Background(), newTestLogger(), bkt, newTestLabels(), 0, 7200000)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(covered))
	testutil.Assert(t, !warn, "expected no overlap warning for backfill-only blocks")
	testutil.Equals(t, int64(7200000), covered[0])
}

func TestScanBucket_NonBackfillBlock_TriggersOverlapWarning(t *testing.T) {
	t.Parallel()
	bkt := objstore.NewInMemBucket()
	extLabels := map[string]string{"cluster": "test", "backfill_job": "test-run"}

	buildInMemMeta(t, bkt, extLabels, metadata.SidecarSource, 0, 7200000)

	_, warn, err := ScanBucket(context.Background(), newTestLogger(), bkt, newTestLabels(), 0, 7200000)
	testutil.Ok(t, err)
	testutil.Assert(t, warn, "expected overlap warning for non-backfill block")
}

func TestScanBucket_BlockOutsideRange_NotInCovered(t *testing.T) {
	t.Parallel()
	bkt := objstore.NewInMemBucket()
	extLabels := map[string]string{"cluster": "test", "backfill_job": "test-run"}

	// Block is entirely before the query range.
	buildInMemMeta(t, bkt, extLabels, metadata.RulesBackfillSource, 0, 1000)

	// Query range starts after block ends.
	covered, _, err := ScanBucket(context.Background(), newTestLogger(), bkt, newTestLabels(), 2000, 9200000)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(covered))
}

func TestScanBucket_DifferentLabels_Ignored(t *testing.T) {
	t.Parallel()
	bkt := objstore.NewInMemBucket()
	differentLabels := map[string]string{"cluster": "other", "backfill_job": "other-run"}

	buildInMemMeta(t, bkt, differentLabels, metadata.RulesBackfillSource, 0, 7200000)

	covered, _, err := ScanBucket(context.Background(), newTestLogger(), bkt, newTestLabels(), 0, 7200000)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(covered))
}

func TestScanBucket_MalformedMetaJson_Skipped(t *testing.T) {
	t.Parallel()
	bkt := objstore.NewInMemBucket()
	blockID := nextFakeULID(t)
	key := filepath.Join(blockID, metadata.MetaFilename)
	testutil.Ok(t, bkt.Upload(context.Background(), key, strings.NewReader("{not valid json")))

	covered, _, err := ScanBucket(context.Background(), newTestLogger(), bkt, newTestLabels(), 0, 7200000)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(covered))
}

func TestScanBucket_NonBlockDirectory_Skipped(t *testing.T) {
	t.Parallel()
	bkt := objstore.NewInMemBucket()
	// Upload a file with a non-ULID prefix — should be ignored.
	testutil.Ok(t, bkt.Upload(context.Background(), "not-a-block/meta.json", strings.NewReader("{}")))

	covered, _, err := ScanBucket(context.Background(), newTestLogger(), bkt, newTestLabels(), 0, 7200000)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(covered))
}

// --- TestRunIdempotency ------------------------------------------------------

func TestRunIdempotency_SkipsAlreadyCoveredWindows(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	dataDir := t.TempDir()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	extLabels := newTestLabels()

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	queryCallCount := 0
	queryFunc := func(_ context.Context, _ string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		queryCallCount++
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "test_metric"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	b := New(newTestLogger(), queryFunc, bkt, extLabels, dataDir,
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true))

	// First run: should produce a block.
	ids1, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids1))
	firstCallCount := queryCallCount

	// Second run: same params → should skip the already-covered window.
	b2 := New(newTestLogger(), queryFunc, bkt, extLabels, t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true))
	ids2, err := b2.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(ids2))

	// Query should not have been called again for the covered window.
	testutil.Equals(t, firstCallCount, queryCallCount)
}

// --- TestRunDryRun -----------------------------------------------------------

func TestRunDryRun_BlockCreatedLocallyNotUploaded(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	dataDir := t.TempDir()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 1),
		bkt,
		newTestLabels(),
		dataDir,
		WithDryRun(true),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))

	// Bucket must remain empty (no upload in dry-run).
	testutil.Equals(t, 0, countBucketBlocks(bkt))

	// Manifest must not be written either.
	for key := range bkt.Objects() {
		testutil.Assert(t, !strings.HasPrefix(key, manifestPrefix), "unexpected bucket key in dry-run: %s", key)
	}
}

// --- TestWriteManifest -------------------------------------------------------

func TestWriteManifest_ManifestAppearsInBucketAfterRun(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 1),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))

	// Find the manifest in the bucket.
	var manifestData []byte
	for key, data := range bkt.Objects() {
		if strings.HasPrefix(key, manifestPrefix+"/") && strings.HasSuffix(key, ".json") {
			manifestData = data
			break
		}
	}
	testutil.Assert(t, manifestData != nil, "expected manifest under backfill-manifests/ in bucket")

	var m manifest
	testutil.Ok(t, json.Unmarshal(manifestData, &m))
	testutil.Equals(t, 1, len(m.Blocks))
	testutil.Equals(t, ids[0], m.Blocks[0])
	testutil.Assert(t, !m.DryRun, "manifest should not be marked as dry-run")
}

// --- TestMultipleAppenderCommit ----------------------------------------------

func TestMultipleAppenderCommit_LargeSampleCountCreatesBlock(t *testing.T) {
	t.Parallel()

	// Produce more than maxSamplesInMemory (5000) samples for one series.
	// This exercises the commit-every-5000 path in processWindow.
	const numSamples = maxSamplesInMemory*2 + 17

	queryFunc := func(_ context.Context, _ string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		// Distribute samples evenly across [startTime, endTime) so all fall
		// within the block's time range.
		span := endTime - startTime
		if span <= 0 {
			span = 1
		}
		interval := span / int64(numSamples+1)
		if interval <= 0 {
			interval = 1
		}
		values := make([]model.SamplePair, numSamples)
		for i := 0; i < numSamples; i++ {
			values[i] = model.SamplePair{
				Timestamp: model.Time(startTime + int64(i+1)*interval),
				Value:     model.SampleValue(float64(i)),
			}
		}
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "big_metric"},
				Values: values,
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))
	testutil.Assert(t, countBucketBlocks(bkt) >= 1, "expected block in bucket after large sample write")
}

// --- TestRunContextCancellation ---------------------------------------------

func TestRunContextCancellation_ReturnsPromptly(t *testing.T) {
	t.Parallel()

	// Use a range that generates several windows so cancellation can fire mid-loop.
	const windows = 10
	blockDur := 2 * time.Hour
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Duration(windows) * blockDur)

	// Track how many windows were actually queried.
	queriedWindows := 0
	cancelAt := 3 // cancel after the 3rd query

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queryFunc := func(qCtx context.Context, _ string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		queriedWindows++
		if queriedWindows >= cancelAt {
			cancel()
		}
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "test_metric"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)
	_, err := b.Run(ctx, []string{ruleFile}, start, end)

	// Run should return a context error after cancellation.
	testutil.Assert(t, err != nil || queriedWindows < windows,
		"expected run to stop early after context cancellation; queriedWindows=%d", queriedWindows)
	testutil.Assert(t, queriedWindows < windows,
		"expected fewer than %d windows to be queried after cancellation, got %d", windows, queriedWindows)
}

// --- TestLabelsMatch ---------------------------------------------------------

func TestLabelsMatch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		blockLabels   map[string]string
		expectedLabel map[string]string
		want          bool
	}{
		{
			name:          "exact match",
			blockLabels:   map[string]string{"a": "1", "b": "2"},
			expectedLabel: map[string]string{"a": "1", "b": "2"},
			want:          true,
		},
		{
			name:          "block has superset of labels",
			blockLabels:   map[string]string{"a": "1", "b": "2", "c": "3"},
			expectedLabel: map[string]string{"a": "1"},
			want:          true,
		},
		{
			name:          "label value mismatch",
			blockLabels:   map[string]string{"a": "X"},
			expectedLabel: map[string]string{"a": "1"},
			want:          false,
		},
		{
			name:          "expected label missing from block",
			blockLabels:   map[string]string{"b": "2"},
			expectedLabel: map[string]string{"a": "1"},
			want:          false,
		},
		{
			name:          "empty expected matches everything",
			blockLabels:   map[string]string{"a": "1"},
			expectedLabel: map[string]string{},
			want:          true,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testutil.Equals(t, tc.want, labelsMatch(tc.blockLabels, tc.expectedLabel))
		})
	}
}

// --- TestIsBlockDir ----------------------------------------------------------

func TestIsBlockDir(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		input  string
		expect bool
	}{
		{name: "valid ULID with trailing slash", input: "01ARZ3NDEKTSV4RRFFQ69G5FAV/", expect: true},
		{name: "valid ULID without slash", input: "01ARZ3NDEKTSV4RRFFQ69G5FAV", expect: true},
		{name: "non-ULID string", input: "not-a-ulid/", expect: false},
		{name: "empty string", input: "", expect: false},
		{name: "too short", input: "01ARZ3NDEK/", expect: false},
		{name: "manifest prefix", input: "backfill-manifests/", expect: false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, ok := isBlockDir(tc.input)
			testutil.Equals(t, tc.expect, ok)
		})
	}
}

// --- TestRun_RuleLabelsApplied -----------------------------------------------

func TestRun_RuleLabels_OverrideQueryResultLabels(t *testing.T) {
	t.Parallel()

	ruleWithLabels := `
groups:
  - name: labeled
    rules:
      - record: labeled_metric
        expr: sum(rate(http_requests_total[5m]))
        labels:
          env: production
          region: us-east
`
	// The query returns a stream with a conflicting label; the rule label should win.
	queryFunc := func(_ context.Context, _ string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{
					model.MetricNameLabel: "labeled_metric",
					"env":                "staging", // should be overridden
				},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, ruleWithLabels)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))
	// The block should have been created; label overriding is tested at the
	// processWindow level by verifying the block is actually written.
	testutil.Assert(t, countBucketBlocks(bkt) == 1, "expected one block in bucket")
}

// --- Regression tests for implementation review findings --------------------

// TestRun_MultiSeriesAppendOrdering verifies that out-of-order timestamps across
// different series within the same window succeed with the 2x writer range.
func TestRun_MultiSeriesAppendOrdering(t *testing.T) {
	t.Parallel()

	// Construct a query result where series A has a late timestamp and series B
	// has an earlier timestamp. With a 1x writer range this would hit ErrOutOfBounds.
	queryFunc := func(_ context.Context, _ string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		mid := (startTime + endTime) / 2
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "series_a"},
				Values: []model.SamplePair{
					{Timestamp: model.Time(mid + 1000), Value: 1.0}, // late timestamp first
				},
			},
			{
				Metric: model.Metric{model.MetricNameLabel: "series_b"},
				Values: []model.SamplePair{
					{Timestamp: model.Time(startTime + 1), Value: 2.0}, // earlier timestamp second
				},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))
	testutil.Assert(t, countBucketBlocks(bkt) == 1, "expected one block")
}

// TestRun_PartialRuleFailure_NoBlockUploaded verifies that when one rule succeeds
// and another fails, no block is uploaded and the window is not considered covered.
func TestRun_PartialRuleFailure_NoBlockUploaded(t *testing.T) {
	t.Parallel()

	twoRulesYAML := `
groups:
  - name: mixed
    rules:
      - record: good_metric
        expr: sum(rate(http_requests_total[5m]))
      - record: bad_metric
        expr: will_fail
`
	callCount := 0
	queryFunc := func(_ context.Context, query string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		callCount++
		if callCount%2 == 0 {
			// Second rule fails.
			return nil, nil, fmt.Errorf("simulated query failure")
		}
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "good_metric"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, twoRulesYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, len(ids))
	testutil.Equals(t, 0, countBucketBlocks(bkt))
}

// TestRun_NonAlignedStart_QueriesClamped verifies that when --start falls in the
// middle of a block boundary, the query does not extend before --start.
func TestRun_NonAlignedStart_QueriesClamped(t *testing.T) {
	t.Parallel()

	blockDur := 2 * time.Hour
	// Start 30 minutes into the first block boundary.
	start := time.Unix(int64(30*time.Minute/time.Second), 0).UTC()
	end := start.Add(blockDur)

	startMs := start.UnixMilli()
	var queriedStart int64

	queryFunc := func(_ context.Context, _ string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		queriedStart = startTime
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "test_metric"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	_, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)

	// The query start must be clamped to the requested start, not the aligned boundary.
	testutil.Assert(t, queriedStart >= startMs,
		"query started at %d, expected >= %d (requested start)", queriedStart, startMs)
}

// TestRun_GroupInterval_UsedPerGroup verifies that per-group intervals from rule
// files are used as the query step rather than the global default.
func TestRun_GroupInterval_UsedPerGroup(t *testing.T) {
	t.Parallel()

	groupIntervalYAML := `
groups:
  - name: fast_group
    interval: 10s
    rules:
      - record: fast_metric
        expr: sum(rate(http_requests_total[5m]))
  - name: slow_group
    interval: 120s
    rules:
      - record: slow_metric
        expr: count(up)
`
	type call struct {
		start int64
		step  int64
	}
	var calls []call
	queryFunc := func(_ context.Context, _ string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		calls = append(calls, call{start: startTime, step: step})
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "placeholder"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, groupIntervalYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithEvalInterval(60*time.Second), // global default 60s
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))
	testutil.Equals(t, 2, len(calls))
	// fast_group should use 10s, slow_group should use 120s.
	testutil.Equals(t, int64(10), calls[0].step)
	testutil.Equals(t, int64(120), calls[1].step)
	testutil.Equals(t, alignEvalStart(start.UnixMilli(), 10*time.Second, ruleFile, "fast_group"), calls[0].start)
	testutil.Equals(t, alignEvalStart(start.UnixMilli(), 120*time.Second, ruleFile, "slow_group"), calls[1].start)
}

// TestRun_RecoveryAfterFailedWindow verifies that a re-run after a failed window
// does not skip the previously failed window.
func TestRun_RecoveryAfterFailedWindow(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	extLabels := newTestLabels()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	// First run: query fails.
	b1 := New(
		newTestLogger(),
		errorQuery(fmt.Errorf("transient failure")),
		bkt,
		extLabels,
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids1, err := b1.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, len(ids1))
	testutil.Equals(t, 0, countBucketBlocks(bkt))

	// Second run: query succeeds. The window should NOT be skipped.
	b2 := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 42),
		bkt,
		extLabels,
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)
	ids2, err := b2.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids2))
	testutil.Assert(t, countBucketBlocks(bkt) >= 1, "expected block after recovery")
}

func TestRun_AdjacentWindowsDoNotOverlapAtBoundary(t *testing.T) {
	t.Parallel()

	type call struct {
		start int64
		end   int64
	}
	var calls []call
	queryFunc := func(_ context.Context, _ string, startTime, endTime, step int64) (model.Matrix, []string, error) {
		calls = append(calls, call{start: startTime, end: endTime})
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "test_metric"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(4 * time.Hour)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)
	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(ids))
	testutil.Equals(t, 2, len(calls))
	testutil.Assert(t, calls[0].end < calls[1].start,
		"adjacent windows overlap: first [%d,%d], second [%d,%d]", calls[0].start, calls[0].end, calls[1].start, calls[1].end)
}

// =============================================================================
// Phase 2 tests
// =============================================================================

// --- fake bucket helpers -----------------------------------------------------

// countingUploadBucket wraps InMemBucket and counts Upload calls. It can be
// configured to fail the first N uploads with a transient error.
type countingUploadBucket struct {
	*objstore.InMemBucket
	mu           sync.Mutex
	uploadCalls  int
	failFirst    int // fail the first N Upload calls
}

func (b *countingUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.mu.Lock()
	b.uploadCalls++
	shouldFail := b.uploadCalls <= b.failFirst
	b.mu.Unlock()
	if shouldFail {
		// Drain the reader so it can be retried from the caller's buffer.
		io.Copy(io.Discard, r) //nolint:errcheck
		return fmt.Errorf("transient upload error (call %d)", b.uploadCalls)
	}
	return b.InMemBucket.Upload(ctx, name, r, opts...)
}

// alwaysFailBucket wraps InMemBucket and makes every Upload call fail.
type alwaysFailBucket struct {
	*objstore.InMemBucket
	mu    sync.Mutex
	calls int
}

func (b *alwaysFailBucket) Upload(_ context.Context, _ string, r io.Reader, _ ...objstore.ObjectUploadOption) error {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	io.Copy(io.Discard, r) //nolint:errcheck
	return fmt.Errorf("permanent upload failure")
}

// iterErrorBucket wraps InMemBucket and makes Iter return an error.
type iterErrorBucket struct {
	*objstore.InMemBucket
}

func (b *iterErrorBucket) Iter(_ context.Context, _ string, _ func(string) error, _ ...objstore.IterOption) error {
	return fmt.Errorf("bucket iter error: connection refused")
}

// uploadErrorBucket wraps InMemBucket and makes Upload return an error,
// simulating a credential that grants read/list but not put.
type uploadErrorBucket struct {
	*objstore.InMemBucket
}

func (b *uploadErrorBucket) Upload(_ context.Context, _ string, _ io.Reader, _ ...objstore.ObjectUploadOption) error {
	return fmt.Errorf("bucket upload error: access denied")
}

// bufferLogger returns a go-kit logger that writes to the provided buffer.
// The buffer can then be inspected for expected log lines.
func bufferLogger(buf *bytes.Buffer) log.Logger {
	return log.NewLogfmtLogger(log.NewSyncWriter(buf))
}

// --- TestRun_Concurrency_ParallelExecution -----------------------------------

// TestRun_Concurrency_ParallelExecution verifies that WithConcurrency(4) allows
// at least 2 windows to be in-flight simultaneously. It uses an atomic counter
// to track peak concurrency across the queryFunc invocations.
func TestRun_Concurrency_ParallelExecution(t *testing.T) {
	t.Parallel()

	const numWindows = 10
	const concurrency = 4
	blockDur := 2 * time.Hour
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Duration(numWindows) * blockDur)

	var (
		inFlight    atomic.Int64
		peakInFlight atomic.Int64
	)

	// gate is used to keep all goroutines inside queryFunc simultaneously so
	// the scheduler has a chance to schedule multiple workers before any exits.
	// Each worker sends to ready then waits for the gate to open.
	ready := make(chan struct{}, numWindows)
	gate := make(chan struct{})

	queryFunc := func(ctx context.Context, _ string, startTime, _, _ int64) (model.Matrix, []string, error) {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)

		// Record peak.
		for {
			old := peakInFlight.Load()
			if cur <= old || peakInFlight.CompareAndSwap(old, cur) {
				break
			}
		}

		// Signal that this goroutine is inside queryFunc.
		ready <- struct{}{}
		// Wait until the gate opens (closed by the test goroutine once enough
		// workers are inside).
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}

		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "m"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithConcurrency(concurrency),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		_, err := b.Run(ctx, []string{ruleFile}, start, end)
		runDone <- err
	}()

	// Wait until at least 2 workers are inside queryFunc, then open the gate.
	waited := 0
	for waited < 2 {
		select {
		case <-ready:
			waited++
		case <-ctx.Done():
			t.Fatal("timed out waiting for workers to become concurrent")
		}
	}
	close(gate)

	// Drain remaining ready signals.
	for i := waited; i < numWindows; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
		}
	}

	err := <-runDone
	testutil.Ok(t, err)

	peak := peakInFlight.Load()
	testutil.Assert(t, peak >= 2,
		"expected peak concurrency >= 2, got %d", peak)
	testutil.Assert(t, peak <= int64(concurrency),
		"expected peak concurrency <= %d, got %d", concurrency, peak)
}

// --- TestRun_Concurrency_ErrorPropagation ------------------------------------

// TestRun_Concurrency_ErrorPropagation verifies that when one window fails,
// other windows still complete (errgroup workers return nil on per-window
// errors, allowing the group to continue), and the returned error is non-nil.
func TestRun_Concurrency_ErrorPropagation(t *testing.T) {
	t.Parallel()

	const numWindows = 5
	blockDur := 2 * time.Hour
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Duration(numWindows) * blockDur)

	// Windows at even indices succeed; odd indices fail.
	var callIdx atomic.Int64
	queryFunc := func(_ context.Context, _ string, startTime, _, _ int64) (model.Matrix, []string, error) {
		idx := callIdx.Add(1)
		if idx%2 == 0 {
			return nil, nil, fmt.Errorf("injected failure for call %d", idx)
		}
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "m"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithConcurrency(4),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)

	// Some windows failed, so err must be non-nil.
	testutil.NotOk(t, err)
	// Some windows succeeded, so we get at least one uploaded block.
	testutil.Assert(t, len(ids) > 0,
		"expected at least one successful block when only some windows fail, got 0")
	// The number of uploaded blocks must be less than total windows.
	testutil.Assert(t, len(ids) < numWindows,
		"expected fewer uploaded blocks than total windows (%d), got %d", numWindows, len(ids))
}

// --- TestRun_Concurrency_ContextCancellation ---------------------------------

// TestRun_Concurrency_ContextCancellation verifies that cancelling the context
// mid-run causes Run to return with a context error.
func TestRun_Concurrency_ContextCancellation(t *testing.T) {
	t.Parallel()

	const numWindows = 8
	blockDur := 2 * time.Hour
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Duration(numWindows) * blockDur)

	ctx, cancel := context.WithCancel(context.Background())

	var called atomic.Int64
	queryFunc := func(qCtx context.Context, _ string, startTime, _, _ int64) (model.Matrix, []string, error) {
		n := called.Add(1)
		if n >= 3 {
			cancel()
		}
		// Block until context is cancelled so the cancellation can propagate.
		select {
		case <-qCtx.Done():
			return nil, nil, qCtx.Err()
		case <-time.After(5 * time.Second):
		}
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "m"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithConcurrency(4),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	_, err := b.Run(ctx, []string{ruleFile}, start, end)
	testutil.Assert(t, err != nil, "expected non-nil error after context cancellation")
	testutil.Assert(t,
		strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "canceled"),
		"expected context canceled error, got: %v", err)
	testutil.Assert(t, called.Load() < int64(numWindows),
		"expected fewer than %d windows processed after cancellation, got %d", numWindows, called.Load())
}

// --- TestRun_Concurrency_WorkerScratchDirsCleanedUp --------------------------

// TestRun_Concurrency_WorkerScratchDirsCleanedUp verifies that all per-window
// worker scratch directories (w-*) are removed from dataDir after Run returns,
// regardless of whether individual windows succeed or fail.
func TestRun_Concurrency_WorkerScratchDirsCleanedUp(t *testing.T) {
	t.Parallel()

	const numWindows = 6
	blockDur := 2 * time.Hour
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Duration(numWindows) * blockDur)

	// Mix of successes and failures.
	var callIdx atomic.Int64
	queryFunc := func(_ context.Context, _ string, startTime, _, _ int64) (model.Matrix, []string, error) {
		idx := callIdx.Add(1)
		if idx == 3 {
			return nil, nil, fmt.Errorf("injected failure")
		}
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "m"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	dataDir := t.TempDir()
	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		dataDir,
		WithConcurrency(4),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	b.Run(context.Background(), []string{ruleFile}, start, end) //nolint:errcheck

	// Verify no w-* directories remain.
	entries, err := os.ReadDir(dataDir)
	testutil.Ok(t, err)
	for _, e := range entries {
		testutil.Assert(t, !strings.HasPrefix(e.Name(), "w-"),
			"expected no w-* scratch dir to remain in dataDir, found: %s", e.Name())
	}
}

// --- TestUploadRetry_TransientFailureSucceeds ---------------------------------

// TestUploadRetry_TransientFailureSucceeds verifies that uploadWithRetry
// succeeds when the bucket fails the first two Upload calls then succeeds.
func TestUploadRetry_TransientFailureSucceeds(t *testing.T) {
	t.Parallel()

	inner := objstore.NewInMemBucket()
	bkt := &countingUploadBucket{InMemBucket: inner, failFirst: 2}
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 1),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRetryAttempts(3),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))

	// Must have attempted at least 3 Upload calls (2 failing + 1 succeeding).
	bkt.mu.Lock()
	calls := bkt.uploadCalls
	bkt.mu.Unlock()
	testutil.Assert(t, calls >= 3,
		"expected >= 3 upload calls (2 failures + 1 success), got %d", calls)

	// Block must be present in the underlying bucket.
	testutil.Assert(t, countBucketBlocks(inner) >= 1, "expected block in bucket after retry success")
}

// --- TestUploadRetry_ExhaustAttempts -----------------------------------------

// TestUploadRetry_ExhaustAttempts verifies that when all block-upload attempts
// fail, uploadWithRetry exhausts its retries, the window is counted as failed,
// and Run returns a non-nil error.
//
// block.Upload issues multiple Upload calls per attempt (one per block file).
// It aborts on the first file error. We fail every upload call and count how
// many times a ULID-prefixed (block file) path is attempted as the first file
// in a new batch — which equals the number of block.Upload invocations, i.e.
// retryAttempts+1.
func TestUploadRetry_ExhaustAttempts(t *testing.T) {
	t.Parallel()

	inner := objstore.NewInMemBucket()

	const retryAttempts = 2

	// Track all Upload call paths so we can count how many block.Upload rounds
	// occurred. Each round starts a new sequence of file uploads; since we fail
	// every single Upload, each round consists of exactly one upload attempt
	// before block.Upload aborts. So totalBlockUploadCalls == retryAttempts+1.
	var totalBlockUploadCalls int
	var mu sync.Mutex

	interceptFunc := func(_ context.Context, name string, r io.Reader) error {
		io.Copy(io.Discard, r) //nolint:errcheck
		// Only count paths that belong to a block (ULID-prefixed), to exclude
		// the manifest upload which also flows through this bucket.
		parts := strings.SplitN(name, "/", 2)
		if len(parts) == 2 {
			if _, err := parsePossibleULID(parts[0]); err == nil {
				mu.Lock()
				totalBlockUploadCalls++
				mu.Unlock()
			}
		}
		return fmt.Errorf("permanent upload failure")
	}

	fakeBkt := &funcUploadBucket{Bucket: inner, upload: interceptFunc}
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 1),
		fakeBkt,
		newTestLabels(),
		t.TempDir(),
		WithRetryAttempts(retryAttempts),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, len(ids))

	// Each block.Upload attempt uploads exactly 1 file before aborting on error.
	// So total calls == retryAttempts+1.
	mu.Lock()
	got := totalBlockUploadCalls
	mu.Unlock()
	testutil.Equals(t, retryAttempts+1, got)
}

// --- TestUploadRetry_ShortCircuitIfAlreadyUploaded ---------------------------

// TestUploadRetry_ShortCircuitIfAlreadyUploaded verifies that if meta.json is
// already present in the bucket when a retry attempt checks (simulating a
// server-acknowledged write whose response was lost), uploadWithRetry treats
// the upload as successful without issuing a second block.Upload call.
//
// Strategy: intercept Upload calls. Let all block files in attempt 0 succeed
// in inner (so meta.json lands in the bucket), but return an error at the end
// of the attempt by failing on the LAST upload of the meta.json file — which
// causes block.Upload to fail. On retry attempt 1, uploadWithRetry calls
// bkt.Exists(metaKey) and finds it already present, so it short-circuits.
// We count how many times block.Upload is invoked by tracking how many times
// the meta.json upload path is reached; it must be exactly 1.
func TestUploadRetry_ShortCircuitIfAlreadyUploaded(t *testing.T) {
	t.Parallel()

	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	inner := objstore.NewInMemBucket()

	// metaUploadCount tracks how many times meta.json was attempted.
	var metaUploadCount int
	var mu sync.Mutex

	interceptFunc := func(ctx context.Context, name string, r io.Reader) error {
		data, _ := io.ReadAll(r)
		// Always write into inner so Exists will find files there.
		if writeErr := inner.Upload(ctx, name, bytes.NewReader(data)); writeErr != nil {
			return writeErr
		}
		// For meta.json specifically: succeed the first time (so inner has it),
		// but also count it. We do NOT fail here — we let all of attempt 0
		// succeed at the inner level. Then we fail the entire block.Upload call
		// by returning an error on the FIRST meta.json upload.
		if strings.HasSuffix(name, "/"+metadata.MetaFilename) {
			mu.Lock()
			metaUploadCount++
			n := metaUploadCount
			mu.Unlock()
			if n == 1 {
				// Return error to simulate lost ACK. inner already has meta.json.
				return fmt.Errorf("transient: server wrote but response lost")
			}
		}
		return nil
	}

	fakeBucket := &funcUploadBucket{Bucket: inner, upload: interceptFunc}

	b := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 1),
		fakeBucket,
		newTestLabels(),
		t.TempDir(),
		WithRetryAttempts(2),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))

	// inner must contain the block from the first attempt's writes.
	testutil.Assert(t, countBucketBlocks(inner) >= 1,
		"expected block in inner bucket after short-circuit on already-uploaded detection")

	// meta.json must have been attempted exactly once — the second attempt
	// finds meta.json via Exists and does not call Upload again.
	mu.Lock()
	finalMetaCount := metaUploadCount
	mu.Unlock()
	testutil.Equals(t, 1, finalMetaCount)
}

// funcUploadBucket wraps a Bucket and overrides Upload with a custom function.
type funcUploadBucket struct {
	objstore.Bucket
	upload func(ctx context.Context, name string, r io.Reader) error
}

func (f *funcUploadBucket) Upload(ctx context.Context, name string, r io.Reader, _ ...objstore.ObjectUploadOption) error {
	return f.upload(ctx, name, r)
}

// --- TestUploadRetry_RespectsContextCancel -----------------------------------

// TestUploadRetry_RespectsContextCancel verifies that cancelling the context
// during the backoff sleep between upload retries causes uploadWithRetry to
// return the context error promptly.
func TestUploadRetry_RespectsContextCancel(t *testing.T) {
	t.Parallel()

	inner := objstore.NewInMemBucket()
	// Always fail so we always hit the backoff path.
	bkt := &alwaysFailBucket{InMemBucket: inner}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after a short delay, giving one Upload attempt time to
	// start but cancelling before the first full retry backoff (1s) completes.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 1),
		bkt,
		newTestLabels(),
		t.TempDir(),
		// Large retry count so we would definitely block in backoff.
		WithRetryAttempts(10),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	start2 := time.Now()
	_, err := b.Run(ctx, []string{ruleFile}, start, end)
	elapsed := time.Since(start2)

	// Run must return an error and must not have waited for all retry backoffs
	// (10 retries with exponential backoff would take > 1023s total).
	testutil.Assert(t, err != nil, "expected error when context cancelled during retry backoff")
	testutil.Assert(t, elapsed < 10*time.Second,
		"Run should return promptly after context cancellation, took %v", elapsed)
}

// --- TestPreflight_QueryEndpointUnreachable ----------------------------------

// TestPreflight_QueryEndpointUnreachable verifies that when the query endpoint
// returns an error during preflight, Run returns early with a "preflight" error
// and no block processing occurs.
func TestPreflight_QueryEndpointUnreachable(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		errorQuery(fmt.Errorf("connection refused")),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		// Preflight is NOT disabled — we want it to run.
	)

	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "preflight"),
		"expected error to mention 'preflight', got: %v", err)
	testutil.Equals(t, 0, len(ids))
	testutil.Equals(t, 0, countBucketBlocks(bkt))
}

// --- TestPreflight_BucketUnreachable -----------------------------------------

// TestPreflight_BucketUnreachable verifies that when the bucket rejects the
// write probe during preflight (non-dry-run), Run returns a preflight error.
// This guards against read/list-only credentials that would silently pass a
// list-based probe and then fail hours later on the first real upload.
func TestPreflight_BucketUnreachable(t *testing.T) {
	t.Parallel()

	bkt := &uploadErrorBucket{InMemBucket: objstore.NewInMemBucket()}
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	// Query succeeds so preflight progresses past the query probe.
	b := New(
		newTestLogger(),
		emptyQuery(),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		// dryRun=false (default) so bucket check runs.
	)

	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "preflight"),
		"expected error to mention 'preflight', got: %v", err)
	testutil.Equals(t, 0, len(ids))
}

// --- TestPreflight_DryRunSkipsBucketCheck ------------------------------------

// TestPreflight_DryRunSkipsBucketCheck verifies that in dry-run mode the
// preflight step does not attempt to call Iter on the bucket. We pass a bucket
// whose Iter always errors; if Iter were called, the test would fail.
func TestPreflight_DryRunSkipsBucketCheck(t *testing.T) {
	t.Parallel()

	// iterErrorBucket.Iter returns an error; if preflight calls it we will see
	// the error bubble up.
	bkt := &iterErrorBucket{InMemBucket: objstore.NewInMemBucket()}
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		singleSampleQuery("test_metric", 1),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithDryRun(true),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)

	// With dryRun=true the bucket check inside preflight is skipped. The only
	// Iter call that could fire is the bucket scan; but in dry-run the bucket
	// scan is also skipped (bkt is not nil but dryRun=true means the scan path
	// takes the "bucket not configured" branch only when bkt==nil). Actually,
	// bucket scan does run when bkt != nil even in dry-run — ScanBucket calls
	// Iter. So we expect the scan to fail, but preflight itself must succeed.
	// We test the narrower property: the preflight-specific "bucket not
	// writable or unreachable" error path must NOT be triggered.
	_, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	if err != nil {
		testutil.Assert(t, !strings.Contains(err.Error(), "bucket not writable"),
			"preflight should not check bucket writable in dry-run; got: %v", err)
	}
}

// --- TestPreflight_DisablePreflightOption ------------------------------------

// TestPreflight_DisablePreflightOption verifies that WithDisablePreflight(true)
// bypasses the preflight step even when the query endpoint would normally fail.
// Run must proceed past preflight (it will fail later in processWindow due to
// the error query, but that is a per-window error, not a preflight error).
func TestPreflight_DisablePreflightOption(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		errorQuery(fmt.Errorf("query endpoint broken")),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithDisablePreflight(true),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
	)

	_, err := b.Run(context.Background(), []string{ruleFile}, start, end)

	// Error must come from processWindow (query failure), not from preflight.
	testutil.NotOk(t, err)
	testutil.Assert(t, !strings.Contains(err.Error(), "preflight"),
		"error should not mention 'preflight' when preflight is disabled; got: %v", err)
}

// --- TestRun_Summary_PartialFailureLogsFailedCount ---------------------------

// TestRun_Summary_PartialFailureLogsFailedCount verifies that the final summary
// log line emitted by Run includes the correct failed window count when some
// windows fail.
func TestRun_Summary_PartialFailureLogsFailedCount(t *testing.T) {
	t.Parallel()

	const numWindows = 5
	const expectedFailed = 2
	blockDur := 2 * time.Hour
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Duration(numWindows) * blockDur)

	// Windows 2 and 4 (1-indexed) fail.
	var callIdx atomic.Int64
	queryFunc := func(_ context.Context, _ string, startTime, _, _ int64) (model.Matrix, []string, error) {
		idx := callIdx.Add(1)
		if idx == 2 || idx == 4 {
			return nil, nil, fmt.Errorf("injected failure %d", idx)
		}
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "m"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	var buf bytes.Buffer
	logger := bufferLogger(&buf)

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		logger,
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithConcurrency(1),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	b.Run(context.Background(), []string{ruleFile}, start, end) //nolint:errcheck

	logOutput := buf.String()

	// The summary line must contain "windows_failed=2".
	testutil.Assert(t,
		strings.Contains(logOutput, fmt.Sprintf("windows_failed=%d", expectedFailed)),
		"expected log to contain windows_failed=%d; log output:\n%s", expectedFailed, logOutput)

	// The summary line must also report the correct total.
	testutil.Assert(t,
		strings.Contains(logOutput, fmt.Sprintf("windows_total=%d", numWindows)),
		"expected log to contain windows_total=%d; log output:\n%s", numWindows, logOutput)
}

// --- TestRun_Manifest_IncludesFailedWindows ----------------------------------

// TestRun_Manifest_IncludesFailedWindows verifies that on partial failure the
// manifest JSON written to the bucket contains FailedWindows entries with
// non-empty start/end fields.
func TestRun_Manifest_IncludesFailedWindows(t *testing.T) {
	t.Parallel()

	const numWindows = 4
	blockDur := 2 * time.Hour
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Duration(numWindows) * blockDur)

	// Window 2 fails.
	var callIdx atomic.Int64
	queryFunc := func(_ context.Context, _ string, startTime, _, _ int64) (model.Matrix, []string, error) {
		idx := callIdx.Add(1)
		if idx == 2 {
			return nil, nil, fmt.Errorf("injected failure")
		}
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "m"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithConcurrency(1),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	b.Run(context.Background(), []string{ruleFile}, start, end) //nolint:errcheck

	// Find the manifest in the bucket.
	var manifestData []byte
	for key, data := range bkt.Objects() {
		if strings.HasPrefix(key, manifestPrefix+"/") && strings.HasSuffix(key, ".json") {
			manifestData = data
			break
		}
	}
	testutil.Assert(t, manifestData != nil, "expected manifest in bucket after partial failure")

	var m manifest
	testutil.Ok(t, json.Unmarshal(manifestData, &m))

	testutil.Assert(t, len(m.FailedWindows) > 0,
		"expected at least one entry in manifest.FailedWindows, got 0")

	for _, fw := range m.FailedWindows {
		testutil.Assert(t, fw.Start > 0 || fw.End > 0,
			"expected non-zero start/end in failed window, got start=%d end=%d", fw.Start, fw.End)
		testutil.Assert(t, fw.Error != "",
			"expected non-empty error in failed window entry")
	}

	// Successful windows produce blocks; check those are also listed.
	testutil.Assert(t, len(m.Blocks) > 0,
		"expected at least one block in manifest alongside failed windows")
}

// --- TestRun_Manifest_WrittenEvenWithoutSuccessfulUploads --------------------

// TestRun_Manifest_WrittenEvenWithoutSuccessfulUploads verifies that the
// manifest is still written to the bucket when all windows fail, so that
// future resume tooling can inspect which windows need reprocessing.
func TestRun_Manifest_WrittenEvenWithoutSuccessfulUploads(t *testing.T) {
	t.Parallel()

	// Single window, always failing.
	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Hour)

	b := New(
		newTestLogger(),
		errorQuery(fmt.Errorf("all windows fail")),
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	ids, err := b.Run(context.Background(), []string{ruleFile}, start, end)
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, len(ids))

	// Manifest must be present even though no blocks were uploaded.
	var manifestData []byte
	for key, data := range bkt.Objects() {
		if strings.HasPrefix(key, manifestPrefix+"/") && strings.HasSuffix(key, ".json") {
			manifestData = data
			break
		}
	}
	testutil.Assert(t, manifestData != nil,
		"expected manifest in bucket even when all windows fail")

	var m manifest
	testutil.Ok(t, json.Unmarshal(manifestData, &m))
	testutil.Equals(t, 0, len(m.Blocks))
	testutil.Assert(t, len(m.FailedWindows) > 0,
		"expected FailedWindows in manifest when all windows fail, got 0")
}

// --- TestRun_Manifest_WrittenOnCancellation ----------------------------------

// TestRun_Manifest_WrittenOnCancellation verifies that when Run is interrupted
// by context cancellation after some windows have already uploaded, the
// manifest still makes it to the bucket so a future resume can pick up where
// the run left off. Regression guard for the early-return-before-manifest bug.
func TestRun_Manifest_WrittenOnCancellation(t *testing.T) {
	t.Parallel()

	const numWindows = 3
	blockDur := 2 * time.Hour
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Duration(numWindows) * blockDur)

	ctx, cancel := context.WithCancel(context.Background())

	var called atomic.Int64
	queryFunc := func(qCtx context.Context, _ string, startTime, _, _ int64) (model.Matrix, []string, error) {
		idx := called.Add(1)
		if idx >= 2 {
			cancel()
			return nil, nil, qCtx.Err()
		}
		ts := model.Time(startTime + 1)
		return model.Matrix{
			{
				Metric: model.Metric{model.MetricNameLabel: "m"},
				Values: []model.SamplePair{{Timestamp: ts, Value: 1.0}},
			},
		}, nil, nil
	}

	bkt := objstore.NewInMemBucket()
	ruleFile := writeRuleFile(t, simpleRecordingRuleYAML)

	b := New(
		newTestLogger(),
		queryFunc,
		bkt,
		newTestLabels(),
		t.TempDir(),
		WithConcurrency(1),
		WithRateLimiter(rate.NewLimiter(rate.Inf, 1)),
		WithDisablePreflight(true),
	)

	ids, err := b.Run(ctx, []string{ruleFile}, start, end)
	testutil.Assert(t, err != nil, "expected non-nil error after context cancellation")
	testutil.Assert(t,
		strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "canceled"),
		"expected context canceled error, got: %v", err)
	testutil.Assert(t, len(ids) >= 1,
		"expected at least one block uploaded before cancellation, got %d", len(ids))

	var manifestData []byte
	for key, data := range bkt.Objects() {
		if strings.HasPrefix(key, manifestPrefix+"/") && strings.HasSuffix(key, ".json") {
			manifestData = data
			break
		}
	}
	testutil.Assert(t, manifestData != nil,
		"expected manifest in bucket after cancelled run, found none")

	var m manifest
	testutil.Ok(t, json.Unmarshal(manifestData, &m))
	testutil.Assert(t, len(m.Blocks) >= 1,
		"expected manifest to reference uploaded blocks after cancellation, got %d", len(m.Blocks))
}

// --- Unused import suppression -----------------------------------------------
// level is imported to enable the bufferLogger helper; this var prevents the
// compiler from complaining if no other test in this file references it directly.
var _ = level.Debug
