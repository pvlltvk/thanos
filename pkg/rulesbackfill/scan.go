// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rulesbackfill

import (
	"context"
	"encoding/json"
	"io"
	"path"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// ScanBucket scans the bucket for already-completed backfill blocks.
// Returns a set of covered time windows (minTime -> maxTime pairs) and a
// warning flag if live-data blocks overlap the target range.
func ScanBucket(ctx context.Context, logger log.Logger, bkt objstore.Bucket, externalLabels labels.Labels, start, end int64) (covered map[int64]int64, overlapWarning bool, err error) {
	covered = make(map[int64]int64)

	expectedLabels := externalLabels.Map()

	err = bkt.Iter(ctx, "", func(name string) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		id, ok := isBlockDir(name)
		if !ok {
			return nil
		}

		metaPath := path.Join(id.String(), metadata.MetaFilename)
		r, getErr := bkt.Get(ctx, metaPath)
		if getErr != nil {
			level.Debug(logger).Log("msg", "failed to get meta.json, skipping", "block", id, "err", getErr)
			return nil
		}
		defer runutil.CloseWithLogOnErr(logger, r, "close meta reader")

		data, readErr := io.ReadAll(r)
		if readErr != nil {
			level.Debug(logger).Log("msg", "failed to read meta.json, skipping", "block", id, "err", readErr)
			return nil
		}

		var meta metadata.Meta
		if jsonErr := json.Unmarshal(data, &meta); jsonErr != nil {
			level.Debug(logger).Log("msg", "failed to parse meta.json, skipping", "block", id, "err", jsonErr)
			return nil
		}

		if !labelsMatch(meta.Thanos.Labels, expectedLabels) {
			return nil
		}

		blockMin := meta.MinTime
		blockMax := meta.MaxTime

		// Check if this block's time range overlaps [start, end).
		if blockMax <= start || blockMin >= end {
			return nil
		}

		if meta.Thanos.Source == metadata.RulesBackfillSource {
			// Track as covered window.
			if existing, ok := covered[blockMin]; !ok || blockMax > existing {
				covered[blockMin] = blockMax
			}
		} else {
			overlapWarning = true
		}

		return nil
	})
	if err != nil {
		return nil, false, errors.Wrap(err, "iterate bucket")
	}

	level.Info(logger).Log("msg", "bucket scan completed", "covered_windows", len(covered), "overlap_warning", overlapWarning)
	return covered, overlapWarning, nil
}

// isBlockDir checks if the given name looks like a ULID directory entry.
// Bucket Iter returns entries with trailing slash for directories.
func isBlockDir(name string) (ulid.ULID, bool) {
	// Strip trailing slash.
	clean := path.Base(name)
	id, err := ulid.Parse(clean)
	if err != nil {
		return ulid.ULID{}, false
	}
	return id, true
}

// labelsMatch returns true if blockLabels contain all expected labels with matching values.
func labelsMatch(blockLabels, expectedLabels map[string]string) bool {
	for k, v := range expectedLabels {
		if blockLabels[k] != v {
			return false
		}
	}
	return true
}
