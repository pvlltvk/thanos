// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rulesbackfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/tsdb"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	maxSamplesInMemory = 5000

	// manifestPrefix is the bucket path prefix for backfill manifests.
	manifestPrefix = "backfill-manifests"

	// progressInterval is how often the periodic progress logger fires.
	progressInterval = 30 * time.Second
)

// QueryRangeFunc abstracts the Thanos Query HTTP API for testing.
// startTime and endTime are in milliseconds; step is in seconds.
type QueryRangeFunc func(ctx context.Context, query string, startTime, endTime, step int64) (model.Matrix, []string, error)

// Backfiller evaluates recording rules against historical data via Thanos Query
// and produces TSDB blocks that are uploaded to object storage.
type Backfiller struct {
	logger           log.Logger
	queryFunc        QueryRangeFunc
	bkt              objstore.Bucket
	externalLabels   labels.Labels
	maxBlockDur      time.Duration
	evalInterval     time.Duration
	dryRun           bool
	hashFunc         metadata.HashFunc
	dataDir          string
	rateLimiter      *rate.Limiter
	concurrency      int
	retryAttempts    int
	disablePreflight bool
}

// Option configures a Backfiller.
type Option func(*Backfiller)

// WithMaxBlockDuration sets the maximum block duration. Default is 2h.
func WithMaxBlockDuration(d time.Duration) Option {
	return func(b *Backfiller) { b.maxBlockDur = d }
}

// WithEvalInterval sets the evaluation interval. Default is 60s.
func WithEvalInterval(d time.Duration) Option {
	return func(b *Backfiller) { b.evalInterval = d }
}

// WithDryRun enables dry-run mode (blocks are created locally but not uploaded).
func WithDryRun(v bool) Option {
	return func(b *Backfiller) { b.dryRun = v }
}

// WithHashFunc sets the hash function for block upload verification.
func WithHashFunc(hf metadata.HashFunc) Option {
	return func(b *Backfiller) { b.hashFunc = hf }
}

// WithRateLimiter sets a custom rate limiter for query requests.
func WithRateLimiter(l *rate.Limiter) Option {
	return func(b *Backfiller) { b.rateLimiter = l }
}

// WithConcurrency sets the maximum number of windows processed in parallel.
// Values <= 0 are treated as 1 (serial). Default is 1.
func WithConcurrency(n int) Option {
	return func(b *Backfiller) {
		if n < 1 {
			n = 1
		}
		b.concurrency = n
	}
}

// WithRetryAttempts sets the number of retry attempts for retriable operations
// (notably block uploads). The caller-supplied queryFunc is expected to have
// its own retry behavior. Default is 0 (no retries).
func WithRetryAttempts(n int) Option {
	return func(b *Backfiller) {
		if n < 0 {
			n = 0
		}
		b.retryAttempts = n
	}
}

// WithDisablePreflight disables the pre-flight validation step. Intended for
// use by unit tests that do not want preflight to issue an extra query or
// bucket operation.
func WithDisablePreflight(disable bool) Option {
	return func(b *Backfiller) { b.disablePreflight = disable }
}

// New creates a new Backfiller with the given configuration.
func New(
	logger log.Logger,
	queryFunc QueryRangeFunc,
	bkt objstore.Bucket,
	externalLabels labels.Labels,
	dataDir string,
	opts ...Option,
) *Backfiller {
	b := &Backfiller{
		logger:         logger,
		queryFunc:      queryFunc,
		bkt:            bkt,
		externalLabels: externalLabels,
		dataDir:        dataDir,
		maxBlockDur:    2 * time.Hour,
		evalInterval:   60 * time.Second,
		dryRun:         false,
		hashFunc:       metadata.NoneFunc,
		rateLimiter:    rate.NewLimiter(rate.Limit(10), 1),
		concurrency:    1,
		retryAttempts:  0,
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// recordingRule is a simplified representation of a recording rule extracted from a rule group.
type recordingRule struct {
	Name   string
	Expr   string
	Labels map[string]string
}

// ruleGroup holds a group name and its recording rules.
type ruleGroup struct {
	Name     string
	File     string
	Interval time.Duration // Per-group evaluation interval; 0 means use the global default.
	Rules    []recordingRule
}

// failedWindow records a window that did not complete successfully during a
// backfill run. Persisted in the manifest to enable future resume tooling.
type failedWindow struct {
	Start int64  `json:"start_ms"`
	End   int64  `json:"end_ms"`
	Error string `json:"error,omitempty"`
}

// manifest is the JSON structure written to the bucket after a backfill run.
type manifest struct {
	Timestamp     time.Time      `json:"timestamp"`
	Blocks        []ulid.ULID    `json:"blocks"`
	Start         int64          `json:"start_ms"`
	End           int64          `json:"end_ms"`
	DryRun        bool           `json:"dry_run"`
	FailedWindows []failedWindow `json:"failed_windows,omitempty"`
}

// Run executes the backfill. It parses rule files, computes block-aligned time
// windows, queries historical data for each recording rule, writes TSDB blocks,
// and uploads them to object storage. Returns the list of uploaded block ULIDs.
func (b *Backfiller) Run(ctx context.Context, ruleFiles []string, start, end time.Time) ([]ulid.ULID, error) {
	startTime := time.Now()

	groups, err := b.parseRuleFiles(ruleFiles)
	if err != nil {
		return nil, errors.Wrap(err, "parse rule files")
	}

	totalRules := 0
	for _, g := range groups {
		totalRules += len(g.Rules)
	}
	if totalRules == 0 {
		return nil, errors.New("no recording rules found in rule files (alerting rules are skipped)")
	}
	level.Info(b.logger).Log("msg", "parsed recording rules", "groups", len(groups), "rules", totalRules)

	startMs := start.UnixMilli()
	endMs := end.UnixMilli()

	blockDurMs := getCompatibleBlockDuration(b.maxBlockDur.Milliseconds())
	windows := computeWindows(startMs, endMs, blockDurMs)

	level.Info(b.logger).Log(
		"msg", "computed block windows",
		"windows", len(windows),
		"block_duration", time.Duration(blockDurMs)*time.Millisecond,
		"start", start.UTC().Format(time.RFC3339),
		"end", end.UTC().Format(time.RFC3339),
	)

	// Pre-flight validation: verify query endpoint and, in non-dry-run mode, bucket accessibility.
	if !b.disablePreflight {
		if err := b.preflight(ctx, len(groups), totalRules, len(windows), endMs); err != nil {
			return nil, errors.Wrap(err, "preflight")
		}
	}

	// Scan bucket for already-completed windows (skip when bucket is nil, e.g. dry-run).
	var (
		covered        = make(map[int64]int64)
		overlapWarning bool
	)
	if b.bkt != nil {
		var scanErr error
		covered, overlapWarning, scanErr = ScanBucket(ctx, b.logger, b.bkt, b.externalLabels, startMs, endMs)
		if scanErr != nil {
			level.Warn(b.logger).Log("msg", "bucket scan failed, proceeding without idempotency check", "err", scanErr)
			covered = make(map[int64]int64)
		}
		if overlapWarning {
			level.Warn(b.logger).Log("msg", "detected existing non-backfill blocks overlapping the target time range; backfilled data may conflict with live data")
		}
	} else {
		level.Info(b.logger).Log("msg", "bucket not configured, skipping bucket scan")
	}

	// Determine pending windows (those not already covered). Idempotency check runs
	// in the main goroutine before spawning workers to keep behavior identical at
	// concurrency=1 and avoid racing against the covered map.
	type pendingWindow struct {
		idx int
		w   window
	}
	var pending []pendingWindow
	for i, w := range windows {
		if isWindowCovered(covered, w.start, w.end) {
			level.Info(b.logger).Log("msg", "skipping already covered window", "start", w.start, "end", w.end)
			continue
		}
		pending = append(pending, pendingWindow{idx: i, w: w})
	}

	concurrency := b.concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	totalWindows := int64(len(windows))
	skippedWindows := int64(len(windows) - len(pending))

	var (
		written atomic.Int64
		failed  atomic.Int64
		skipped atomic.Int64
	)
	skipped.Store(skippedWindows)

	// Progress reporter.
	progressCtx, progressCancel := context.WithCancel(ctx)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		t := time.NewTicker(progressInterval)
		defer t.Stop()
		for {
			select {
			case <-progressCtx.Done():
				return
			case <-t.C:
				logProgress(b.logger, written.Load(), skipped.Load(), failed.Load(), totalWindows)
			}
		}
	}()

	// Shared mutable state protected by mu.
	var (
		mu          sync.Mutex
		uploaded    []ulid.ULID
		merr        errutil.MultiError
		failures    = make(map[string]int)
		failedList  []failedWindow
	)

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)

	for _, pw := range pending {
		pw := pw
		eg.Go(func() error {
			if egCtx.Err() != nil {
				return egCtx.Err()
			}

			// Each worker gets its own dataDir subdir so concurrent BlockWriters
			// do not collide on intermediate state files. Cleaned up on return.
			workDir := filepath.Join(b.dataDir, fmt.Sprintf("w-%d", pw.idx))
			if err := os.MkdirAll(workDir, 0o755); err != nil {
				failed.Add(1)
				mu.Lock()
				merr.Add(errors.Wrapf(err, "create worker dir for window [%d, %d)", pw.w.start, pw.w.end))
				failedList = append(failedList, failedWindow{Start: pw.w.start, End: pw.w.end, Error: err.Error()})
				mu.Unlock()
				return nil
			}
			defer func() {
				if rmErr := os.RemoveAll(workDir); rmErr != nil {
					level.Warn(b.logger).Log("msg", "failed to remove worker dir", "dir", workDir, "err", rmErr)
				}
			}()

			id, perRule, err := b.processWindow(egCtx, groups, pw.w.start, pw.w.end, blockDurMs, startMs, endMs, workDir)
			if err != nil {
				// Context cancellation propagates through the errgroup and stops the run.
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				failed.Add(1)
				mu.Lock()
				merr.Add(errors.Wrapf(err, "process window [%d, %d)", pw.w.start, pw.w.end))
				failedList = append(failedList, failedWindow{Start: pw.w.start, End: pw.w.end, Error: err.Error()})
				if perRule != "" {
					failures[perRule]++
				}
				mu.Unlock()
				return nil
			}

			written.Add(1)
			if id != (ulid.ULID{}) {
				mu.Lock()
				uploaded = append(uploaded, id)
				mu.Unlock()
			}
			return nil
		})
	}

	runErr := eg.Wait()
	progressCancel()
	<-progressDone

	// Final progress line before the summary.
	logProgress(b.logger, written.Load(), skipped.Load(), failed.Load(), totalWindows)

	// Per-rule failure breakdown.
	mu.Lock()
	if len(failures) > 0 {
		for k, v := range failures {
			level.Warn(b.logger).Log("msg", "rule failure count", "rule", k, "failed_windows", v)
		}
	}
	mu.Unlock()

	// If errgroup returned early due to context cancellation, surface it.
	if runErr != nil && (errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)) {
		mu.Lock()
		u := append([]ulid.ULID(nil), uploaded...)
		mu.Unlock()
		level.Info(b.logger).Log("msg", "backfill cancelled",
			"windows_total", totalWindows,
			"windows_written", written.Load(),
			"windows_skipped", skipped.Load(),
			"windows_failed", failed.Load(),
			"blocks_uploaded", len(u),
			"duration", time.Since(startTime),
		)
		return u, runErr
	}

	// Write manifest to bucket.
	mu.Lock()
	uploadedCopy := append([]ulid.ULID(nil), uploaded...)
	failedCopy := append([]failedWindow(nil), failedList...)
	mu.Unlock()

	if !b.dryRun && b.bkt != nil && (len(uploadedCopy) > 0 || len(failedCopy) > 0) {
		if err := b.writeManifest(ctx, uploadedCopy, startMs, endMs, failedCopy); err != nil {
			level.Warn(b.logger).Log("msg", "failed to write backfill manifest", "err", err)
		}
	}

	level.Info(b.logger).Log("msg", "backfill summary",
		"windows_total", totalWindows,
		"windows_written", written.Load(),
		"windows_skipped", skipped.Load(),
		"windows_failed", failed.Load(),
		"blocks_uploaded", len(uploadedCopy),
		"duration", time.Since(startTime),
	)

	return uploadedCopy, merr.Err()
}

// logProgress emits a single progress log line with the given counters.
func logProgress(logger log.Logger, written, skipped, failed, total int64) {
	pct := float64(0)
	if total > 0 {
		pct = float64(written+skipped+failed) / float64(total) * 100
	}
	level.Info(logger).Log(
		"msg", "progress",
		"written", written,
		"skipped", skipped,
		"failed", failed,
		"total", total,
		"percent", fmt.Sprintf("%.1f", pct),
	)
}

// preflight validates that the query endpoint is reachable, that there are
// recording rules to process, and, in non-dry-run mode, that the bucket is
// accessible. It also logs estimated work up front.
func (b *Backfiller) preflight(ctx context.Context, numGroups, totalRules, numWindows int, endMs int64) error {
	// Probe the query endpoint with a cheap 1-minute range query so we fail fast
	// if the endpoint is unreachable, misconfigured, or returning errors.
	probeStart := endMs - 60_000
	probeEnd := endMs
	if probeStart < 0 {
		probeStart = 0
	}
	if _, _, err := b.queryFunc(ctx, "up", probeStart, probeEnd, 60); err != nil {
		return errors.Wrap(err, "query endpoint unreachable or returning errors")
	}

	// In non-dry-run mode, exercise bucket auth and reachability without mutating state.
	// Iter with a non-existent prefix is cheap and should not return an error just
	// because nothing is there.
	if !b.dryRun && b.bkt != nil {
		if err := b.bkt.Iter(ctx, "preflight-check-nonexistent/", func(string) error { return nil }); err != nil {
			return errors.Wrap(err, "bucket not writable or unreachable")
		}
	}

	level.Info(b.logger).Log(
		"msg", "preflight complete",
		"rules", totalRules,
		"groups", numGroups,
		"windows", numWindows,
		"estimated_queries", totalRules*numWindows,
	)
	return nil
}

// parseRuleFiles parses all provided rule file globs and extracts recording rules.
func (b *Backfiller) parseRuleFiles(patterns []string) ([]ruleGroup, error) {
	var groups []ruleGroup

	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, errors.Wrapf(err, "glob pattern %q", pattern)
		}
		if len(matches) == 0 {
			return nil, errors.Errorf("no files matched pattern %q", pattern)
		}

		for _, file := range matches {
			rgs, errs := rulefmt.ParseFile(file, false, model.UTF8Validation)
			if len(errs) > 0 {
				var me errutil.MultiError
				for _, e := range errs {
					me.Add(e)
				}
				return nil, errors.Wrapf(me.Err(), "parse rule file %s", file)
			}

			for _, rg := range rgs.Groups {
				var rules []recordingRule
				for _, r := range rg.Rules {
					if r.Record == "" {
						continue // Skip alerting rules.
					}
					rules = append(rules, recordingRule{
						Name:   r.Record,
						Expr:   r.Expr,
						Labels: r.Labels,
					})
				}
					if len(rules) > 0 {
						var groupInterval time.Duration
						if rg.Interval != 0 {
							groupInterval = time.Duration(rg.Interval)
						}
						groups = append(groups, ruleGroup{
							Name:     rg.Name,
							File:     file,
							Interval: groupInterval,
							Rules:    rules,
						})
					}
				}
		}
	}
	return groups, nil
}

// processWindow creates a TSDB block for a single time window.
// requestedStart and requestedEnd are the user-specified boundaries used to clamp
// the query range so we never query data outside the requested interval.
// workDir is a per-window scratch directory owned by the caller; processWindow
// does not remove it. The second return value is a "group/rule" key identifying
// the rule that failed, if the failure is attributable to a specific rule.
func (b *Backfiller) processWindow(ctx context.Context, groups []ruleGroup, windowStart, windowEnd, blockDurMs, requestedStart, requestedEnd int64, workDir string) (ulid.ULID, string, error) {
	// Clamp query range to the user-requested interval.
	queryStart := maxInt64(windowStart, requestedStart)
	queryEndExclusive := minInt64(windowEnd, requestedEnd)
	if queryStart >= queryEndExclusive {
		level.Debug(b.logger).Log("msg", "clamped window is empty, skipping", "window_start", windowStart, "window_end", windowEnd)
		return ulid.ULID{}, "", nil
	}
	// PromQL query_range end is inclusive while our planned windows are half-open [start, end).
	// Convert to an inclusive end here so adjacent windows do not duplicate boundary samples.
	queryEndInclusive := queryEndExclusive - 1

	slogLogger := logutil.GoKitLogToSlog(b.logger)
	// Use 2*blockDurMs for the writer range. BlockWriter rejects samples older than
	// the appendable minimum time once the head advances. With multiple series appending
	// out-of-order timestamps within the same window, a 1x range can hit ErrOutOfBounds.
	// The doubled range avoids this; Flush still writes correct min/max from actual samples.
	writer, err := tsdb.NewBlockWriter(slogLogger, workDir, 2*blockDurMs)
	if err != nil {
		return ulid.ULID{}, "", errors.Wrap(err, "create block writer")
	}
	defer runutil.CloseWithLogOnErr(b.logger, writer, "block writer")

	sampleCount := 0
	totalSamples := 0
	app := writer.Appender(ctx)

	for _, group := range groups {
		// Use the group's own interval if set, otherwise fall back to the global default.
		evalInterval := b.evalInterval
		if group.Interval > 0 {
			evalInterval = group.Interval
		}
		stepSeconds := int64(evalInterval.Seconds())
		alignedStart := alignEvalStart(queryStart, evalInterval, group.File, group.Name)
		if alignedStart > queryEndInclusive {
			level.Debug(b.logger).Log(
				"msg", "aligned query range is empty for group, skipping",
				"group", group.Name,
				"query_start", queryStart,
				"aligned_start", alignedStart,
				"query_end", queryEndInclusive,
			)
			continue
		}

		for _, rule := range group.Rules {
			ruleKey := group.Name + "/" + rule.Name
			if ctx.Err() != nil {
				return ulid.ULID{}, "", ctx.Err()
			}

			if err := b.rateLimiter.Wait(ctx); err != nil {
				return ulid.ULID{}, ruleKey, errors.Wrap(err, "rate limiter wait")
			}

			level.Debug(b.logger).Log(
				"msg", "querying rule",
				"group", group.Name,
				"rule", rule.Name,
				"expr", rule.Expr,
				"query_start", alignedStart,
				"query_end", queryEndInclusive,
				"step", stepSeconds,
			)

			matrix, warnings, queryErr := b.queryFunc(ctx, rule.Expr, alignedStart, queryEndInclusive, stepSeconds)
			if queryErr != nil {
				// Fail the entire window on any rule query failure.
				if rollbackErr := app.Rollback(); rollbackErr != nil {
					level.Warn(b.logger).Log("msg", "rollback after query failure", "err", rollbackErr)
				}
				return ulid.ULID{}, ruleKey, errors.Wrapf(queryErr, "query failed for rule %s/%s", group.Name, rule.Name)
			}
			for _, w := range warnings {
				level.Warn(b.logger).Log("msg", "query warning", "rule", rule.Name, "warning", w)
			}

			for _, stream := range matrix {
				// Build label set: start with the metric labels from the query result,
				// then overlay any additional labels defined on the rule.
				metricLabels := make(map[string]string, len(stream.Metric)+len(rule.Labels)+1)
				for k, v := range stream.Metric {
					metricLabels[string(k)] = string(v)
				}
				// Set __name__ to the recording rule name.
				metricLabels[labels.MetricName] = rule.Name
				// Apply rule-level labels (they override query result labels).
				for k, v := range rule.Labels {
					metricLabels[k] = v
				}
				lbls := labels.FromMap(metricLabels)

				for _, sp := range stream.Values {
					ts := int64(sp.Timestamp)
					val := float64(sp.Value)

					if _, appendErr := app.Append(0, lbls, ts, val); appendErr != nil {
						// Fail the entire window on any append failure.
						if rollbackErr := app.Rollback(); rollbackErr != nil {
							level.Warn(b.logger).Log("msg", "rollback after append failure", "err", rollbackErr)
						}
						return ulid.ULID{}, ruleKey, errors.Wrapf(appendErr, "append sample for rule %s/%s at ts %d", group.Name, rule.Name, ts)
					}
					sampleCount++
					totalSamples++

					if sampleCount >= maxSamplesInMemory {
						if commitErr := app.Commit(); commitErr != nil {
							return ulid.ULID{}, ruleKey, errors.Wrap(commitErr, "commit appender")
						}
						app = writer.Appender(ctx)
						sampleCount = 0
					}
				}
			}
		}
	}

	// Final commit for remaining samples.
	if sampleCount > 0 {
		if err := app.Commit(); err != nil {
			return ulid.ULID{}, "", errors.Wrap(err, "final commit")
		}
	} else {
		if err := app.Rollback(); err != nil {
			level.Warn(b.logger).Log("msg", "rollback empty appender", "err", err)
		}
	}

	if totalSamples == 0 {
		level.Info(b.logger).Log("msg", "no samples in window, skipping block creation", "start", windowStart, "end", windowEnd)
		return ulid.ULID{}, "", nil
	}

	blockID, err := writer.Flush(ctx)
	if err != nil {
		return ulid.ULID{}, "", errors.Wrap(err, "flush block writer")
	}

	blockDir := filepath.Join(workDir, blockID.String())
	level.Info(b.logger).Log(
		"msg", "block created",
		"block", blockID,
		"samples", totalSamples,
		"window_start", windowStart,
		"window_end", windowEnd,
	)

	// Inject Thanos metadata into the block.
	thanosMeta := metadata.Thanos{
		Labels: b.externalLabels.Map(),
		Source: metadata.RulesBackfillSource,
		Downsample: metadata.ThanosDownsample{
			Resolution: 0,
		},
	}
	if _, err := metadata.InjectThanos(b.logger, blockDir, thanosMeta, nil); err != nil {
		return ulid.ULID{}, "", errors.Wrap(err, "inject thanos metadata")
	}

	if b.dryRun {
		level.Info(b.logger).Log("msg", "dry-run: block written locally, skipping upload", "block", blockID, "dir", blockDir)
		return blockID, "", nil
	}

	// Upload to object storage with bounded retry + exponential backoff.
	if err := b.uploadWithRetry(ctx, blockID, blockDir); err != nil {
		return ulid.ULID{}, "", errors.Wrap(err, "upload block")
	}
	level.Info(b.logger).Log("msg", "block uploaded", "block", blockID)

	// Clean up local block directory after successful upload.
	if err := os.RemoveAll(blockDir); err != nil {
		level.Warn(b.logger).Log("msg", "failed to remove local block dir after upload", "dir", blockDir, "err", err)
	}

	return blockID, "", nil
}

// uploadWithRetry uploads a block directory with bounded exponential backoff.
// Before each retry it checks whether the block's meta.json already exists in
// the bucket (e.g. from a prior attempt that finished uploading but whose
// response was lost) and treats that as success to avoid duplicate uploads.
func (b *Backfiller) uploadWithRetry(ctx context.Context, blockID ulid.ULID, blockDir string) error {
	metaKey := path.Join(blockID.String(), metadata.MetaFilename)
	var lastErr error
	for attempt := 0; attempt <= b.retryAttempts; attempt++ {
		if attempt > 0 {
			// Check if a prior attempt actually completed (eventual-consistency / lost response).
			if exists, exErr := b.bkt.Exists(ctx, metaKey); exErr == nil && exists {
				level.Info(b.logger).Log("msg", "block already exists in bucket, treating upload as success", "block", blockID)
				return nil
			}
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second // 1s, 2s, 4s, ...
			level.Debug(b.logger).Log("msg", "retrying upload", "block", blockID, "attempt", attempt, "backoff", backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		lastErr = block.Upload(ctx, b.logger, b.bkt, blockDir, b.hashFunc)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return errors.Wrapf(lastErr, "upload failed after %d attempts", b.retryAttempts+1)
}

// writeManifest writes a JSON manifest listing all uploaded block ULIDs and any
// failed windows to the bucket. It is written even on partial failure so future
// tooling (e.g. a resume subcommand) can inspect which windows still need work.
func (b *Backfiller) writeManifest(ctx context.Context, blockIDs []ulid.ULID, startMs, endMs int64, failedWindows []failedWindow) error {
	m := manifest{
		Timestamp:     time.Now().UTC(),
		Blocks:        blockIDs,
		Start:         startMs,
		End:           endMs,
		DryRun:        b.dryRun,
		FailedWindows: failedWindows,
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal manifest")
	}

	key := fmt.Sprintf("%s/%s.json", manifestPrefix, m.Timestamp.Format("20060102T150405Z"))
	return b.bkt.Upload(ctx, key, bytes.NewReader(data))
}

// window represents a half-open time window [start, end) in milliseconds.
type window struct {
	start int64
	end   int64
}

// isWindowCovered returns true if any block in the covered set overlaps with the
// window [wStart, wEnd). A backfill block whose samples fall within a window indicates
// that window was already processed. Block MinTime/MaxTime come from actual sample
// timestamps and will not align exactly with window boundaries.
func isWindowCovered(covered map[int64]int64, wStart, wEnd int64) bool {
	for blockMin, blockMax := range covered {
		// Two ranges overlap when each starts before the other ends.
		if blockMin < wEnd && blockMax > wStart {
			return true
		}
	}
	return false
}

// computeWindows divides [startMs, endMs) into block-aligned windows of blockDurMs.
func computeWindows(startMs, endMs, blockDurMs int64) []window {
	// Align start down to block boundary.
	alignedStart := (startMs / blockDurMs) * blockDurMs

	var windows []window
	for ws := alignedStart; ws < endMs; ws += blockDurMs {
		we := ws + blockDurMs
		if we > endMs {
			we = endMs
		}
		windows = append(windows, window{start: ws, end: we})
	}
	return windows
}

// getCompatibleBlockDuration returns the largest block duration from TSDB's exponential
// ranges that does not exceed maxDurMs and divides evenly into maxDurMs.
func getCompatibleBlockDuration(maxDurMs int64) int64 {
	if maxDurMs <= 0 {
		return tsdb.DefaultBlockDuration
	}

	ranges := tsdb.ExponentialBlockRanges(tsdb.DefaultBlockDuration, 10, 3)
	sort.Slice(ranges, func(i, j int) bool { return ranges[i] < ranges[j] })

	result := tsdb.DefaultBlockDuration
	for _, r := range ranges {
		if r > maxDurMs {
			break
		}
		if maxDurMs%r == 0 {
			result = r
		}
	}
	return result
}

func alignEvalStart(queryStartMs int64, interval time.Duration, file, group string) int64 {
	if interval <= 0 {
		return queryStartMs
	}

	intervalNs := int64(interval)
	offset := int64(labels.New(
		labels.Label{Name: "name", Value: group},
		labels.Label{Name: "file", Value: file},
	).Hash() % uint64(intervalNs))

	startNs := time.UnixMilli(queryStartMs).UTC().UnixNano()
	adjNow := startNs - offset
	base := adjNow - (adjNow % intervalNs)
	next := base + offset

	aligned := time.Unix(0, next).UTC()
	for aligned.UnixMilli() < queryStartMs {
		aligned = aligned.Add(interval)
	}
	return aligned.UnixMilli()
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
