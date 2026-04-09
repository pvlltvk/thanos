// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rulesbackfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/tsdb"
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
)

// QueryRangeFunc abstracts the Thanos Query HTTP API for testing.
// startTime and endTime are in milliseconds; step is in seconds.
type QueryRangeFunc func(ctx context.Context, query string, startTime, endTime, step int64) (model.Matrix, []string, error)

// Backfiller evaluates recording rules against historical data via Thanos Query
// and produces TSDB blocks that are uploaded to object storage.
type Backfiller struct {
	logger         log.Logger
	queryFunc      QueryRangeFunc
	bkt            objstore.Bucket
	externalLabels labels.Labels
	maxBlockDur    time.Duration
	evalInterval   time.Duration
	dryRun         bool
	hashFunc       metadata.HashFunc
	dataDir        string
	rateLimiter    *rate.Limiter
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

// manifest is the JSON structure written to the bucket after a backfill run.
type manifest struct {
	Timestamp time.Time   `json:"timestamp"`
	Blocks    []ulid.ULID `json:"blocks"`
	Start     int64       `json:"start_ms"`
	End       int64       `json:"end_ms"`
	DryRun    bool        `json:"dry_run"`
}

// Run executes the backfill. It parses rule files, computes block-aligned time
// windows, queries historical data for each recording rule, writes TSDB blocks,
// and uploads them to object storage. Returns the list of uploaded block ULIDs.
func (b *Backfiller) Run(ctx context.Context, ruleFiles []string, start, end time.Time) ([]ulid.ULID, error) {
	groups, err := b.parseRuleFiles(ruleFiles)
	if err != nil {
		return nil, errors.Wrap(err, "parse rule files")
	}

	if len(groups) == 0 {
		level.Warn(b.logger).Log("msg", "no recording rules found in provided rule files")
		return nil, nil
	}

	totalRules := 0
	for _, g := range groups {
		totalRules += len(g.Rules)
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

	var (
		uploaded []ulid.ULID
		merr     errutil.MultiError
	)

	for _, w := range windows {
		if ctx.Err() != nil {
			return uploaded, ctx.Err()
		}

		// Skip windows already covered by previous backfill runs.
		if isWindowCovered(covered, w.start, w.end) {
			level.Info(b.logger).Log("msg", "skipping already covered window", "start", w.start, "end", w.end)
			continue
		}

		id, err := b.processWindow(ctx, groups, w.start, w.end, blockDurMs, startMs, endMs)
		if err != nil {
			merr.Add(errors.Wrapf(err, "process window [%d, %d)", w.start, w.end))
			continue
		}
		if id != (ulid.ULID{}) {
			uploaded = append(uploaded, id)
		}
	}

	// Write manifest to bucket.
	if len(uploaded) > 0 && !b.dryRun {
		if err := b.writeManifest(ctx, uploaded, startMs, endMs); err != nil {
			level.Warn(b.logger).Log("msg", "failed to write backfill manifest", "err", err)
		}
	}

	level.Info(b.logger).Log("msg", "backfill completed", "blocks_created", len(uploaded), "errors", len(merr))
	return uploaded, merr.Err()
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
func (b *Backfiller) processWindow(ctx context.Context, groups []ruleGroup, windowStart, windowEnd, blockDurMs, requestedStart, requestedEnd int64) (ulid.ULID, error) {
	// Clamp query range to the user-requested interval.
	queryStart := maxInt64(windowStart, requestedStart)
	queryEndExclusive := minInt64(windowEnd, requestedEnd)
	if queryStart >= queryEndExclusive {
		level.Debug(b.logger).Log("msg", "clamped window is empty, skipping", "window_start", windowStart, "window_end", windowEnd)
		return ulid.ULID{}, nil
	}
	// PromQL query_range end is inclusive while our planned windows are half-open [start, end).
	// Convert to an inclusive end here so adjacent windows do not duplicate boundary samples.
	queryEndInclusive := queryEndExclusive - 1

	slogLogger := logutil.GoKitLogToSlog(b.logger)
	// Use 2*blockDurMs for the writer range. BlockWriter rejects samples older than
	// the appendable minimum time once the head advances. With multiple series appending
	// out-of-order timestamps within the same window, a 1x range can hit ErrOutOfBounds.
	// The doubled range avoids this; Flush still writes correct min/max from actual samples.
	writer, err := tsdb.NewBlockWriter(slogLogger, b.dataDir, 2*blockDurMs)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "create block writer")
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
			if ctx.Err() != nil {
				return ulid.ULID{}, ctx.Err()
			}

			if err := b.rateLimiter.Wait(ctx); err != nil {
				return ulid.ULID{}, errors.Wrap(err, "rate limiter wait")
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
				return ulid.ULID{}, errors.Wrapf(queryErr, "query failed for rule %s/%s", group.Name, rule.Name)
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
						return ulid.ULID{}, errors.Wrapf(appendErr, "append sample for rule %s/%s at ts %d", group.Name, rule.Name, ts)
					}
					sampleCount++
					totalSamples++

					if sampleCount >= maxSamplesInMemory {
						if commitErr := app.Commit(); commitErr != nil {
							return ulid.ULID{}, errors.Wrap(commitErr, "commit appender")
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
			return ulid.ULID{}, errors.Wrap(err, "final commit")
		}
	} else {
		if err := app.Rollback(); err != nil {
			level.Warn(b.logger).Log("msg", "rollback empty appender", "err", err)
		}
	}

	if totalSamples == 0 {
		level.Info(b.logger).Log("msg", "no samples in window, skipping block creation", "start", windowStart, "end", windowEnd)
		return ulid.ULID{}, nil
	}

	blockID, err := writer.Flush(ctx)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "flush block writer")
	}

	blockDir := filepath.Join(b.dataDir, blockID.String())
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
		return ulid.ULID{}, errors.Wrap(err, "inject thanos metadata")
	}

	if b.dryRun {
		level.Info(b.logger).Log("msg", "dry-run: block written locally, skipping upload", "block", blockID, "dir", blockDir)
		return blockID, nil
	}

	// Upload to object storage.
	if err := block.Upload(ctx, b.logger, b.bkt, blockDir, b.hashFunc); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "upload block")
	}
	level.Info(b.logger).Log("msg", "block uploaded", "block", blockID)

	// Clean up local block directory after successful upload.
	if err := os.RemoveAll(blockDir); err != nil {
		level.Warn(b.logger).Log("msg", "failed to remove local block dir after upload", "dir", blockDir, "err", err)
	}

	return blockID, nil
}

// writeManifest writes a JSON manifest listing all uploaded block ULIDs to the bucket.
func (b *Backfiller) writeManifest(ctx context.Context, blockIDs []ulid.ULID, startMs, endMs int64) error {
	m := manifest{
		Timestamp: time.Now().UTC(),
		Blocks:    blockIDs,
		Start:     startMs,
		End:       endMs,
		DryRun:    b.dryRun,
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
