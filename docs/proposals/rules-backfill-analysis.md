# Thanos Rules Backfill -- Feature Analysis

**Date:** 2026-03-28
**Status:** Proposal
**Author:** Business Analysis

---

## Table of Contents

1. [Feature Overview](#1-feature-overview)
2. [User Stories](#2-user-stories)
3. [Architecture and Design](#3-architecture-and-design)
4. [Parameters and Configuration](#4-parameters-and-configuration)
5. [Technical Considerations](#5-technical-considerations)
6. [Risks and Mitigations](#6-risks-and-mitigations)
7. [Implementation Plan](#7-implementation-plan)
8. [Label Strategy and Idempotency](#8-label-strategy-and-idempotency)
9. [Implementation Review Follow-up](#9-implementation-review-follow-up)

---

## 1. Feature Overview

### What This Feature Does

This feature adds a `thanos tools rules-backfill` subcommand that retroactively evaluates
Prometheus recording rules against historical data accessible through Thanos Query, produces
TSDB blocks from the results, and uploads those blocks directly to an object storage bucket
(S3, GCS, Azure, etc.).

End-to-end flow:

1. User provides one or more Prometheus rule files containing recording rules.
2. User specifies a time range (absolute or relative) and a Thanos Query HTTP endpoint.
3. The tool parses the rule files, extracts recording rules (alerting rules are skipped).
4. For each rule, the tool issues `query_range` HTTP requests against Thanos Query,
   chunked into block-sized time windows.
5. The returned matrix data is written into TSDB blocks on local disk using `tsdb.BlockWriter`.
6. Each completed block is annotated with Thanos-specific metadata (external labels, source type,
   resolution) and uploaded to the configured object storage bucket.
7. Local temporary blocks are cleaned up after successful upload.

### How It Differs from Promtool

| Aspect | Promtool (`tsdb create-blocks-from rules`) | Thanos `tools rules-backfill` |
|---|---|---|
| **Data source** | Single Prometheus instance HTTP API | Thanos Query (fans out across Store GW, Sidecars, Receivers -- full historical data) |
| **Output destination** | Local directory on disk | Object storage bucket (S3/GCS/Azure/Swift/filesystem) |
| **Block metadata** | Standard Prometheus `meta.json` | Thanos-extended `meta.json` with external labels, source type, resolution, file hashes |
| **Multi-tenancy** | Not supported | Tenant ID header passed to Thanos Query; external labels can encode tenant |
| **Deduplication** | Not considered | Must respect Thanos deduplication semantics; external labels must not collide with existing blocks |
| **Scale** | Designed for single-instance data volumes | Must handle months/years of data across a distributed system; needs concurrency controls, chunking, and upload pipelining |
| **Error recovery** | Fails entirely on error | Should support partial progress, resumability, and per-block error isolation |
| **Lifecycle** | Standalone binary | Integrated into Thanos CLI using `oklog/run.Group` and `extkingpin` patterns |

The core algorithmic approach is borrowed from Promtool's `ruleImporter` (see
`/home/pvlltvk/Work/github/prometheus/cmd/promtool/rules.go`), but the surrounding
infrastructure -- querying, metadata, upload, error handling -- is entirely Thanos-specific.

---

## 2. User Stories

### Primary Use Cases

**US-1: Backfill a newly created recording rule**

> As a platform engineer, I want to backfill a new recording rule against the last 6 months
> of data in Thanos, so that dashboards using the new metric have historical data immediately
> instead of only showing data from the moment the rule was deployed.

Acceptance criteria:
- The tool accepts a standard Prometheus rule file and a time range.
- Blocks are generated and uploaded to the same bucket the Store Gateway reads from.
- The new metric is queryable through Thanos Query within minutes of upload (after Store GW
  resync).
- No existing metrics or blocks are modified.

**US-2: Migrate recording rules from one set of labels to another**

> As an SRE, I want to regenerate recording rule output with updated label sets (e.g., after
> renaming a cluster label), so that I preserve historical continuity under the new naming
> convention.

Acceptance criteria:
- External labels on generated blocks match the new naming convention.
- Old blocks remain untouched (user handles cleanup separately).

**US-3: Disaster recovery -- reconstruct aggregated metrics**

> As an infrastructure lead, I want to re-derive aggregated metrics from raw data after a
> compactor failure destroyed some blocks, so that I restore monitoring coverage without
> re-ingesting raw samples.

Acceptance criteria:
- Tool can target a specific time range where data was lost.
- Generated blocks integrate cleanly with existing blocks in the bucket.

**US-4: Populate a new Thanos environment from an existing one**

> As a DevOps engineer migrating to a new Thanos cluster, I want to pre-compute all recording
> rules against the source cluster and upload the results to the destination bucket, so that
> the new environment has full metric history from day one.

Acceptance criteria:
- Tool connects to the source Thanos Query endpoint.
- Blocks are uploaded to a separately configured destination bucket.

### Edge Cases and Failure Scenarios

| Scenario | Expected behavior |
|---|---|
| Rule file contains only alerting rules | Tool warns that no recording rules were found and exits cleanly (exit 0, no blocks produced) |
| Thanos Query is unreachable | Fail fast with a clear connection error before any block creation |
| Query returns partial data (partial response) | Respect Thanos partial response strategy flag; warn user; continue or abort based on configuration |
| Time range exceeds available data | Produce blocks only for the sub-range where data exists; empty time windows produce no blocks |
| Rule depends on output of another rule in the same file | Processing in file order does NOT resolve this within a single pass -- rule A's output is not queryable until uploaded and synced by Store GW. User must run separate backfill invocations. See Section 3 for dependency handling. |
| Object storage write fails mid-upload | `block.Upload` writes `meta.json` last; partial block (chunks+index, no `meta.json`) is invisible to Store GW and compactor. On re-run, the startup bucket scan does not find a completed block for that window and re-processes it with a new ULID. The orphaned partial directory is harmless and cleaned up by the compactor's `BlocksCleaner`. |
| Generated block overlaps with existing block in bucket (same external labels) | Startup scan detects the complete block and skips that window. If user intentionally runs with same labels as live data, compactor requires vertical compaction -- tool warns at pre-flight. |
| Very large time range (1+ year at 15s resolution) | Must chunk into block-sized windows; must limit concurrency and memory; see Section 5 |
| Network timeout during query_range | Retry with exponential backoff (configurable); fail after max retries |

---

## 3. Architecture and Design

### Where This Fits in the Codebase

The new subcommand registers under the existing `tools` command tree:

```
thanos tools rules-backfill \
  --rules="rules/*.yaml" \
  --query="http://thanos-query:9090" \
  --objstore.config-file="bucket.yaml" \
  --start="-180d" \
  --end="now" \
  --label='cluster="prod-us-east-1"'
```

Registration follows the pattern established by `registerCheckRules` in
`/home/pvlltvk/Work/github/pvlltvk/thanos/cmd/thanos/tools.go` and the bucket
sub-commands in `tools_bucket.go`.

### Data Flow

```
                                 Thanos Query
                                 (HTTP API)
                                      ^
                                      | query_range requests
                                      | (chunked by block duration)
                                      |
+------------------+          +-------+--------+         +------------------+
|                  |  parse   |                |  write  |                  |
|  Rule Files      +--------->+  Backfill      +-------->+  Local TSDB      |
|  (*.yaml)        |          |  Engine        |         |  Blocks (tmpdir) |
|                  |          |                |         |                  |
+------------------+          +-------+--------+         +--------+---------+
                                      |                           |
                                      | set Thanos metadata       | upload
                                      | (ext labels, source,      |
                                      |  resolution)              v
                                      |                  +--------+---------+
                                      +----------------->+                  |
                                                         |  Object Storage  |
                                                         |  Bucket          |
                                                         |  (S3/GCS/Azure)  |
                                                         +------------------+
```

### Key Components and Responsibilities

**Component 1: CLI Registration (`cmd/thanos/tools_rules_backfill.go`)**

- Registers `rules-backfill` under the `tools` command.
- Parses and validates all flags.
- Wires up dependencies (logger, bucket client, HTTP client, tracer, registry).
- Uses `cmd.Setup(func(g *run.Group, ...) error { ... })` pattern.

**Component 2: Rule Parser (`pkg/rules/` -- reuse existing)**

- Thanos already has `pkg/rules.ValidateAndCount` used by `tools rules-check`.
- Extend or complement with a function that loads and returns parsed rule groups
  (recording rules only), similar to how Promtool's `ruleImporter.loadGroups` calls
  `rules.Manager.LoadGroups`.
- Leverage the Prometheus `rules` package directly for parsing, since Thanos already
  depends on it.

**Component 3: Backfill Engine (`pkg/rulesbackfill/backfill.go`)**

Core orchestration logic. Responsibilities:
- Accept parsed rule groups, time range, and configuration.
- For each rule, chunk the time range into block-duration-aligned windows (using
  `getCompatibleBlockDuration` logic from Promtool's `backfill.go`).
- For each window, issue a `query_range` call via `pkg/promclient.Client.QueryRange`.
- Write results into a `tsdb.BlockWriter` (same approach as Promtool's `importRule`).
- After flushing, annotate the block's `meta.json` with Thanos metadata.
- Upload the block to the bucket.
- Clean up the local temporary block directory.

**Component 4: Query Client (reuse `pkg/promclient`)**

- `promclient.Client.QueryRange` already provides the exact HTTP interface needed.
- Supports custom headers (for tenant ID injection).
- Supports Thanos-specific query options (dedup, partial response, max resolution).

**Component 5: Block Uploader (reuse `pkg/block.Upload`)**

- `block.Upload` (in `/home/pvlltvk/Work/github/pvlltvk/thanos/pkg/block/block.go:97`)
  handles uploading a block directory to object storage with proper hash verification.
- The Thanos metadata must be written to `meta.json` before calling Upload.

### Handling Large Time Ranges

The time range is divided into block-duration-aligned windows. The default TSDB block
duration is 2 hours (`tsdb.DefaultBlockDuration`). For very large ranges, this means:

- 6 months at 2h blocks = ~2,190 block windows per rule
- 1 year = ~4,380 block windows per rule

Strategy:
1. Process blocks sequentially by default (memory-safe).
2. Optional `--concurrency` flag to process N blocks in parallel (bounded worker pool).
3. Each block window is self-contained: query, write, upload, cleanup. This means memory
   usage is bounded by `concurrency * (query result size + block write buffer)`.
4. Support `--max-block-duration` to allow larger blocks (e.g., 24h, 1w) for coarser
   backfills, reducing the number of API calls and blocks produced.

### Handling Rule Dependencies

If rule B's expression references a metric produced by rule A, and both are in the same
rule file being backfilled, rule B will produce incorrect (empty) results unless rule A's
output is already queryable.

**Recommended approach (Phase 1 -- simple):**
- Process rules in file order within each group (matching Prometheus evaluation order).
- **Important limitation:** Processing in file order does NOT make rule A's output available
  to rule B within the same backfill pass. Unlike live Prometheus evaluation (where rule A's
  output is immediately queryable for rule B within the same group evaluation cycle), the
  backfill tool queries Thanos Query, which only sees data that has already been uploaded
  and synced by the Store Gateway. Therefore, if rule B depends on rule A, the user must
  run backfill for rule A first, wait for the Store GW to pick up the new blocks (default
  sync interval: 3 minutes), then run backfill for rule B in a separate invocation.

**Future enhancement (Phase 2):**
- Add a `--wait-for-upload` mode that, after uploading blocks for a rule, pauses until
  the data is queryable via Thanos Query before proceeding to the next rule.
- Alternatively, build a local in-memory query layer that can resolve recently-backfilled
  data without requiring a round-trip through Thanos.

---

## 4. Parameters and Configuration

### Required Parameters

| Parameter | Flag | Type | Description |
|---|---|---|---|
| Rule files | `--rules` | `[]string` (glob) | One or more Prometheus rule files containing recording rules. Alerting rules are skipped. |
| Start time | `--start` | `TimeOrDurationValue` | Start of backfill range. Accepts RFC3339 (`2025-01-01T00:00:00Z`) or relative Prometheus duration (`-180d`, `-30d`, `-8760h`). Note: `model.ParseDuration` does not support calendar months (`mo`); use days or hours instead. |
| Thanos Query URL | `--query` | `*url.URL` | HTTP(S) endpoint of Thanos Query (e.g., `http://thanos-query:9090`). |
| Object storage config | `--objstore.config` / `--objstore.config-file` | YAML | Standard Thanos object storage configuration (reuse `extkingpin.RegisterCommonObjStoreFlags`). |

### Optional Parameters

| Parameter | Flag | Type | Default | Description |
|---|---|---|---|---|
| End time | `--end` | `TimeOrDurationValue` | 3 hours ago | End of backfill range. Defaults to 3h ago to avoid overlap with live ingestion. |
| Evaluation interval | `--eval-interval` | `duration` | `60s` | Default step interval for rule evaluation (used as `step` in `query_range`). If a rule group specifies its own `interval`, that value takes precedence. This flag only applies to groups without an explicit interval, matching Prometheus semantics where each group can have its own evaluation interval. |
| Max block duration | `--max-block-duration` | `duration` | `2h` | Maximum duration of generated blocks. Rounded to TSDB-compatible boundaries. |
| External labels | `--label` | `[]string` (key=value) | (none) | External labels to attach to generated blocks. **Required** -- `block.Upload` rejects blocks with empty external labels. |
| Tenant ID header | `--tenant-header` | `string` | (none) | Value for the tenant ID HTTP header sent to Thanos Query (for multi-tenant setups). |
| Concurrency | `--concurrency` | `int` | `1` | Number of block windows to process in parallel. |
| Temporary directory | `--tmp-dir` | `string` | OS temp dir | Local directory for intermediate block storage before upload. |
| Deduplication | `--dedup` | `bool` | `true` | Whether to request deduplicated data from Thanos Query. |
| Partial response | `--partial-response` | `bool` | `false` | Accept partial responses from Thanos Query. |
| Max source resolution | `--max-source-resolution` | `string` | `0s` | Maximum source resolution to use when querying (0s = raw, 5m, 1h). |
| Query timeout | `--query-timeout` | `duration` | `5m` | Timeout for individual `query_range` HTTP requests. |
| Retry attempts | `--retry-attempts` | `int` | `3` | Number of retries for failed queries. |
| Dry run | `--dry-run` | `bool` | `false` | Generate blocks locally but do not upload to object storage. |
| Hash function | `--hash-func` | `string` | `""` | Hash function for block verification during upload (e.g., `SHA256`). |
| Block source label | (hardcoded) | `SourceType` | `"rules.backfill"` | Thanos metadata source type for generated blocks. |

### Validation Rules

1. `--start` must be before `--end`.
2. `--start` must not be in the future.
3. `--eval-interval` must be at least 1 second.
4. `--max-block-duration` must be at least 2 hours (TSDB minimum).
5. `--concurrency` must be between 1 and 32.
6. At least one `--label` must be provided (hard error if missing -- `block.Upload` rejects
   blocks with empty external labels).
7. `--rules` glob must match at least one file.
8. Each matched file must contain at least one valid recording rule (warn if only alerting
   rules found).
9. `--query` must be a valid URL and reachable (pre-flight health check via
   `/api/v1/status/config` or similar).

---

## 5. Technical Considerations

### Block Metadata

Generated blocks must have a `meta.json` that conforms to Thanos expectations
(`pkg/block/metadata.Meta`):

```json
{
  "ulid": "<generated>",
  "minTime": 1704067200000,
  "maxTime": 1704074400000,
  "stats": { "numSamples": ..., "numSeries": ..., "numChunks": ... },
  "compaction": { "level": 1, "sources": ["<self-ulid>"] },
  "version": 1,
  "thanos": {
    "version": 1,
    "labels": {
      "cluster": "prod-us-east-1",
      "backfill_job": "2026-03"
    },
    "downsample": { "resolution": 0 },
    "source": "rules.backfill"
  }
}
```

Key decisions:
- **Source type:** Add a new `SourceType` constant `RulesBackfillSource = "rules.backfill"`
  in `pkg/block/metadata/meta.go` (next to existing types like `BucketUploadSource`).
- **Resolution:** Always `0` (raw) since the tool generates original-resolution data.
  The compactor will produce downsampled versions as needed.
- **External labels:** User-provided via `--label` flags. These must uniquely identify
  the block's origin to prevent deduplication conflicts.
- **Compaction level:** 1 (uncompacted). The compactor will compact these blocks normally.

### Deduplication and Conflict Avoidance

The primary use case (US-1) is backfilling a recording rule that never existed before: the
new metric is absent from all existing blocks. There is no content overlap — existing blocks
contain raw metrics; backfilled blocks contain rule output.

However, the compactor's overlap check is **purely time-range based**. Two blocks in the
same compaction group (identical external labels + resolution) with overlapping `[MinTime,
MaxTime)` trigger a conflict regardless of what series they contain. This is the core design
challenge; see Section 8 for the full label strategy analysis and recommendation.

The correct approach is:

1. **User provides a distinguishing external label** (e.g., `--label='backfill="true"'` or
   a job-specific value like `--label='backfill_job="2026-03"'`). This places backfilled
   blocks in their own compaction group, separate from live-ingested data. The extra label
   is visible on series at query time, which is acceptable.
2. **The tool itself never adds labels automatically.** External labels = exactly the user's
   `--label` flags.
3. **The tool performs a startup bucket scan** (see Section 7) so that re-runs after
   interruption never upload duplicate blocks within the backfill group.
4. **If the user intentionally omits any distinguishing label**, blocks land in the same
   group as live data. The pre-flight scan detects this and warns that
   `--compact.enable-vertical-compaction` must be active on the compactor.

### Performance

**Memory usage:**
- Promtool caps in-memory samples at 5,000 (`maxSamplesInMemory` in `rules.go:34`).
  We should use the same pattern via `multipleAppender`.
- Each block window's query result is held in memory briefly. For a 2h window at 60s
  evaluation interval, one rule produces at most 120 samples per series. With 10,000
  series, that is ~1.2M samples (~30 MB). This is manageable.
- Risk: rules that produce very high cardinality output. Mitigation: the `maxSamplesInMemory`
  cap on the appender commits periodically.

**Query load on Thanos:**
- Each block window generates one `query_range` call per rule.
- For 100 rules over 6 months: 100 * 2,190 = 219,000 queries.
- Mitigation: `--concurrency` limits parallelism; `--max-block-duration` reduces query count;
  add rate limiting option if needed.

**Object storage costs:**
- Each 2h block upload involves writing index, chunks, and meta files.
- For 100 rules over 6 months: up to 219,000 blocks (worst case, one per rule per window).
  In practice, multiple rules in the same time window should be batched into a single block.
- **Optimization (important):** Batch all rules within a group into the same block writer
  for each time window, exactly as Promtool does. This reduces block count from
  `rules * windows` to just `windows`.

### Error Handling

| Error type | Handling strategy |
|---|---|
| Rule file parse error | Fail before any queries are made. Report all parse errors at once. |
| Individual query failure | Retry up to `--retry-attempts` with exponential backoff. If all retries fail, log the error, skip that block window, continue with next. Report summary at end. |
| Block write failure (disk) | Fatal for that block window. Log and continue with next window. |
| Upload failure | Retain the local block. Log the error. At end, report list of un-uploaded blocks with paths so user can manually upload or re-run. |
| Partial response from Query | Governed by `--partial-response` flag. If disabled (default), treat as error. If enabled, proceed with partial data and log warning. |
| Context cancellation (SIGINT) | Graceful shutdown via `run.Group`. Finish current block upload if in progress. Report progress. |

### Testing Strategy

**Unit tests:**
- Rule file parsing with various edge cases (mixed recording/alerting, empty files, syntax errors).
- Time range chunking logic (alignment, boundary conditions, single-window ranges).
- Block metadata generation (correct external labels, source type, resolution).
- `multipleAppender` commit/flush behavior.

**Integration tests:**
- Mock HTTP server acting as Thanos Query, returning canned `query_range` responses.
- Verify that TSDB blocks are correctly written to a temporary directory.
- Verify that block `meta.json` contains expected Thanos metadata.
- Test upload to a filesystem-backed objstore bucket.

**End-to-end tests (in `test/e2e/`):**
- Spin up Thanos Query + Store GW + Minio (or filesystem bucket).
- Pre-populate some raw metrics.
- Run `thanos tools rules-backfill` against the Query endpoint.
- Verify that the backfilled metric is queryable through Thanos Query after Store GW resync.

---

## 6. Risks and Mitigations

### Data Consistency Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Overlapping blocks with existing live data (same ext. labels, no vertical compaction) | Medium | High | Pre-flight bucket scan warns if overlapping blocks detected; user must enable vertical compaction on compactor. Downsampled blocks (5m/1h) are in separate compaction groups -- no interference. |
| Intra-backfill overlaps after re-run (same backfill label set, job interrupted and restarted) | High | High | Startup bucket scan detects completed windows (meta.json present, labels and source match) and skips them. Incomplete windows are re-processed with a new ULID. Orphaned partial directories from interrupted runs are not deleted (safe in shared buckets) and are cleaned up by the compactor's `BlocksCleaner`. No vertical compaction needed. |
| Rule dependency ordering produces empty/wrong results | High (if rules depend on each other) | High | Document limitation clearly: file order does not resolve dependencies within a single pass; user must run separate invocations with Store GW sync in between. Phase 2 adds `--wait-for-upload` mode to automate this. |
| Backfilled data differs from what live evaluation would have produced (due to raw data availability, resolution, dedup) | Medium | Low | Document that backfill is "best effort" approximation; resolution and dedup flags let user control behavior |

### Performance Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Backfill overloads Thanos Query | Medium | High | `--concurrency` cap; `--query-rate-limit` (default 10 QPS, included in Phase 1); recommend running during off-peak |
| Excessive object storage API calls | Medium | Medium | Batch rules into single blocks per time window; use larger `--max-block-duration` |
| Local disk fills up during large backfill | Low | High | Upload and delete blocks incrementally (not at end); `--tmp-dir` lets user choose a large volume |
| Very long running job (days for multi-year backfill) | Medium | Medium | Progress logging; startup scan provides automatic resume on re-run |

### Operational Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| User accidentally backfills with wrong external labels | Medium | High | `--dry-run` mode for validation; prominent warning if labels don't match existing blocks in bucket |
| Object storage costs for large backfills | Medium | Medium | Document expected costs; `--dry-run` reports estimated block count |
| Store GW takes time to discover new blocks | Low (expected behavior) | Low | Document expected delay; user can trigger Store GW resync |

---

## 7. Implementation Plan

### Phase 1: Minimum Viable Feature (estimated: 3-4 weeks)

Goal: Working end-to-end backfill for simple recording rules with object storage upload,
including the minimum safety controls required for production use (see Appendix C).

#### File-by-file Breakdown

**New files:**

| File | Description |
|---|---|
| `cmd/thanos/tools_rules_backfill.go` | CLI registration, flag parsing, wiring. Follows the pattern of `registerCheckRules` in `tools.go` and `registerBucketUploadBlocks` in `tools_bucket.go`. ~150-200 lines. |
| `pkg/rulesbackfill/backfill.go` | Core backfill engine: startup bucket scan, time range chunking, query loop, block writing, metadata annotation, upload orchestration. Primary logic file. ~350-450 lines. |
| `pkg/rulesbackfill/scan.go` | Startup bucket scan: lists all blocks, identifies completed windows by reading `meta.json` (matching labels and `source: "rules.backfill"`), detects live-data overlap warnings. Does not delete orphaned directories (safe in shared buckets). ~100-150 lines. |
| `pkg/rulesbackfill/backfill_test.go` | Unit tests for the backfill engine and bucket scan with mock query client and `objstore.NewInMemBucket`. ~250-350 lines. |

**Modified files:**

| File | Change |
|---|---|
| `cmd/thanos/tools.go` | Add `registerRulesBackfill(cmd)` call inside `registerTools`. One line addition. |
| `pkg/block/metadata/meta.go` | Add `RulesBackfillSource SourceType = "rules.backfill"` constant. One line addition. |
| `pkg/component/component.go` | Add `RulesBackfill = source{component: component{name: "rules-backfill"}}` if needed for metrics registration. |

**Dependencies between changes:**
1. Add `SourceType` constant in `metadata/meta.go` (no dependencies).
2. Implement `pkg/rulesbackfill/backfill.go` (depends on 1).
3. Write unit tests for the engine (depends on 2).
4. Implement CLI registration in `cmd/thanos/tools_rules_backfill.go` (depends on 2).
5. Register in `tools.go` (depends on 4).

#### Key Design Decisions for Phase 1

- **Startup bucket scan (required, not optional):** On startup, scan the bucket via `bkt.Iter("")` to build a `covered` set of windows that already have a valid `meta.json` matching the run's external labels and `source: "rules.backfill"`. Skip those windows. Orphaned partial blocks (ULID directory present, no `meta.json`) are **not deleted** during the scan -- the tool only skips completed windows and re-processes incomplete ones by writing to a new ULID. Orphaned directories from prior interrupted runs are harmless (invisible to Store GW and compactor) and will be cleaned up by the compactor's `BlocksCleaner` or manual bucket maintenance. This avoids the risk of deleting in-flight uploads from other components (sidecar, receiver, compactor, or concurrent backfill jobs) that share the same bucket. This provides automatic resume after any interruption without a local checkpoint file.
- **Pre-flight overlap warning:** If the scan finds complete blocks from live-ingested data (same external labels as the run) overlapping the target range, log a prominent warning advising the user to verify `--compact.enable-vertical-compaction` is active.
- Sequential processing only (concurrency = 1).
- One block per time window, batching all rules in a group together.
- No dependency resolution between rules -- process in file order, but this does NOT make
  earlier rules' output available to later rules within the same pass. Document that
  dependent rules require separate invocations with Store GW sync in between.
- Upload each block immediately after creation (streaming approach, not batch-at-end).
- Use `promclient.Client.QueryRange` for all Thanos Query communication.
- Use `tsdb.NewBlockWriter` for block creation (same as Promtool).
- Use `block.Upload` for object storage upload.
- **Dry-run mode (`--dry-run`):** Generate blocks locally and report stats without uploading.
  Required before any production use (see Appendix C).
- **Rate limiting (`--query-rate-limit`):** Token bucket rate limiter (default 10 QPS) on
  queries to Thanos Query. Without this, a large backfill can degrade query availability
  for production users (see Appendix C).
- **Manifest file:** Write a JSON manifest listing all uploaded block ULIDs to a well-known
  path in the bucket. This enables rollback via `thanos tools bucket delete` or a future
  rollback subcommand.
- **Hard validation of `--label`:** Fail at startup if no external labels are provided
  (see Section 8).

### Phase 2: Robustness and Usability (estimated: 1-2 weeks)

Goal: Production-grade error handling, progress reporting, and basic concurrency.

| Feature | Description |
|---|---|
| Concurrent block processing | Bounded worker pool for parallel block creation and upload (`--concurrency` flag). |
| Progress reporting | Periodic log messages: "Processed 150/2190 blocks for rule X (6.8%)" |
| Retry with backoff | Configurable retry for query failures and upload failures. |
| Pre-flight validation | Check Query endpoint reachability; validate that at least one rule file has recording rules; estimate total work. |
| Partial failure summary | At completion, report: N blocks created, M uploaded, K failed (with details). |

### Phase 3: Advanced Features (estimated: 2-3 weeks)

Goal: Handle complex scenarios and improve efficiency.

| Feature | Description |
|---|---|
| Rule dependency awareness | `--wait-for-upload` mode that pauses between rules to let Store GW pick up new blocks. |
| Rollback subcommand | `thanos tools backfill rollback --manifest <path>` writes deletion marks for all blocks listed in a manifest. |
| Native histogram support | Handle native histogram samples in query results (depends on Thanos Query returning them via HTTP API). |
| Multi-tenant batching | Accept a list of tenant IDs and backfill for each, reusing the same rule files. |

### Phase 4: Documentation and E2E Tests (estimated: 1 week)

| Deliverable | Description |
|---|---|
| User documentation | How-to guide with examples: simple backfill, large-scale backfill, multi-tenant backfill. |
| E2E test | Docker-based test in `test/e2e/` that validates the full workflow. |
| Runbook | Operational guide: estimating costs, handling failures, verifying results. |

---

## 8. Label Strategy and Idempotency

### The Problem

Two interrelated problems must be solved together:

1. **Compaction group placement:** Backfilled blocks always cover a time range where the
   bucket already has compacted blocks (up to 14-day level). The compactor's overlap check
   is time-range-only — it doesn't look at series content. Same-group overlap → compactor
   halt (if vertical compaction is off) or expensive full re-merge (if on).

2. **Re-run idempotency:** Backfill jobs can run for hours/days and get interrupted.
   Re-runs must not upload duplicate blocks that would cause intra-group overlaps.

These problems are solved independently: (1) by the label strategy, (2) by the startup
bucket scan.

### Background: How External Labels Work at Query Time

External labels are block-level metadata stored in `meta.json`. At query time, the Store
Gateway **injects external labels into every series** returned from that block. This means:

- A series `my_rule{cluster="prod"}` in a block with extra external label
  `backfill_job="2026-03"` is returned as `my_rule{cluster="prod", backfill_job="2026-03"}`.
- A query for `my_rule{cluster="prod"}` (no matcher on `backfill_job`) **will still match**
  both backfilled and live blocks.
- The extra label is **visible** in query results and Grafana legends. This means the
  backfilled series and the live series have **different label sets** (e.g.,
  `my_rule{cluster="prod", backfill_job="2026-03"}` vs. `my_rule{cluster="prod"}`).
  They are distinct time series. Grafana legends, PromQL joins (`on(...)`), and any logic
  that expects a single stable label set across the backfilled/live boundary will see a
  split at the cutover point. For the primary use case (new rule, never ran live), this is
  acceptable since there is no live counterpart. For US-2 (label migration) or any scenario
  requiring seamless label continuity, use Strategy A (same labels + vertical compaction)
  instead.

### How the Compactor Groups Blocks (confirmed from code analysis)

`Thanos.GroupKey()` at `pkg/block/metadata/meta.go:199`:

```go
fmt.Sprintf("%d@%v", m.Downsample.Resolution, labels.FromMap(m.Labels).Hash())
```

Key confirmed facts:
- **Resolution is the first component.** Raw (0), 5m (300000), 1h (3600000) blocks are
  always in separate compaction groups. Backfilled raw blocks never interact with existing
  downsampled blocks.
- Two blocks with identical external labels and resolution land in the same group. If their
  `[MinTime, MaxTime)` ranges overlap:
  - **Vertical compaction off (default):** `areBlocksOverlapping` fires in `Group.compact()`
    *before* the planner runs → `HaltError` → **entire compactor halts** (not just the
    affected group — all compaction stops for the bucket).
  - **Vertical compaction on:** `selectOverlappingMetas` plans all overlapping blocks together
    regardless of compaction level. All blocks in the plan are fully downloaded and re-merged.
    This is a **one-time cost** — after the merge, the group has no more overlaps.
- The I/O cost is proportional to the **largest block in the plan** (e.g., a 14-day block
  must be fully downloaded even if the backfilled block is only 2h).
- Backfilled blocks always have fresh ULIDs. The `compaction.sources` check in
  `PreCompactionCallback` never triggers for fresh backfill blocks.

### Strategy A: Same External Labels (opt-in)

Backfilled blocks carry the exact same labels as live data. Same compaction group.

**When to use:** The operator explicitly wants no extra label on series AND has already
enabled `--compact.enable-vertical-compaction` on the compactor.

**Risk:** If vertical compaction is NOT active when the first backfill block lands, the
compactor halts on its next run, stopping ALL compaction for the entire bucket. The
startup scan will detect this situation and emit a prominent warning.

**Vertical compaction behavior (confirmed from code):** The compactor downloads all
overlapping blocks (including the full 14-day existing block), re-merges them into one
output block, then marks inputs for deletion. This is a **one-time cost per affected
time range** — after the merge, subsequent compaction runs are normal. Backfilled series
(new recording rule output) are cleanly embedded in the merged block alongside existing
series, since there is no series-level conflict.

### Strategy B: Distinct External Labels (recommended default)

The user adds at least one distinguishing label via `--label`. Examples:
- `--label='backfill_job="2026-03"'` — unique per job run (preferred)
- `--label='backfill="true"'` — simple permanent marker

**Queryability:** A query for `my_rule{cluster="prod"}` still matches backfilled blocks.
The extra label is visible on series, meaning backfilled and live series have different
label sets and are distinct time series (see "How External Labels Work at Query Time"
above for implications). For the primary use case (new rule never ran live), there is no
duplicate data — the metric simply didn't exist in live blocks before. For use cases that
require label continuity across the backfilled/live boundary, use Strategy A instead.

**Intra-job safety:** The startup bucket scan ensures a re-run after interruption never
uploads a duplicate block for a window that already completed. No vertical compaction
needed within a single job's compaction group.

**Multiple jobs:** If multiple jobs share the same backfill label value (e.g., all use
`backfill="true"`) and their time ranges overlap, the backfill group accumulates overlapping
blocks that need vertical compaction to resolve. **Using a unique label value per job**
(e.g., `backfill_job="2026-03-my-rule"`) gives each job its own compaction group and
eliminates this concern entirely.

### Comparison

| Criterion | Strategy A (Same Labels) | Strategy B (Distinct Labels, recommended) |
|---|---|---|
| **Extra label visible on series** | No | Yes (user-controlled value) |
| **Compactor safety without vertical compaction** | Halts entire bucket | Safe — separate group |
| **Multiple jobs same label value** | Requires vertical compaction | Requires vert. compaction OR unique value per job |
| **Intra-job re-run safety** | Startup scan skips completed windows | Startup scan skips completed windows |
| **Downsampled block interaction** | None — resolution makes separate GroupKey | None — resolution makes separate GroupKey |
| **Prerequisite compactor change** | Yes — vertical compaction must be enabled | No |

### Recommendation

**Use Strategy B as the default. Use unique label values per job.**

The tool does NOT add any label automatically. Recommended practice:

```bash
# Preferred: unique per job — fully safe, no compactor changes needed
thanos tools rules-backfill \
  --label='cluster="prod"' \
  --label='backfill_job="2026-03-rules"' \
  ...

# Also fine: simple marker, safe for a single non-overlapping job
thanos tools rules-backfill \
  --label='cluster="prod"' \
  --label='backfill="true"' \
  ...

# Advanced: same labels as live data — requires vertical compaction on compactor
thanos tools rules-backfill \
  --label='cluster="prod"' \
  ...
```

The `thanos.source: "rules.backfill"` field provides traceability regardless of strategy.

The CLI should:
1. Hard-error at startup if no `--label` flags are provided. `block.Upload` rejects blocks
   with empty external labels, so allowing the job to proceed without labels wastes time
   querying and writing blocks that will fail at upload.
2. Warn prominently if the startup scan finds live-data blocks with the same GroupKey — vertical compaction must be active on the compactor.
3. Beyond the empty-labels check, never block the upload — the user's label choice is respected.

### Idempotency: Startup Bucket Scan

On every invocation, before any block is written, the tool scans the bucket. The bucket
IS the checkpoint — no local state file is needed.

**Algorithm:**

```
1. Compute all planned windows W_1..W_N (deterministic: same --start/--end/--max-block-duration
   always produces the same splits).

2. bkt.Iter("") → collect all top-level ULID directories.

3. For each ULID directory:
   a. Attempt bkt.Get(<ulid>/meta.json).
      - Found, labels match run's external labels, source is "rules.backfill",
        [MinTime,MaxTime] in target range:
          → add (MinTime, MaxTime) to covered set.
      - Found, labels match a live-data group (same GroupKey without backfill label):
          → emit overlap warning: "existing live-data blocks detected in target range;
            ensure --compact.enable-vertical-compaction is active on the compactor."
      - Not found (no meta.json):
          → skip. Do NOT delete. The directory may belong to another component
            (sidecar, receiver, compactor, or concurrent backfill job) that is
            mid-upload. Orphaned directories are invisible to Store GW and compactor
            and will be cleaned up by the compactor's BlocksCleaner.

4. For each window W_i:
   - W_i in covered → log "skipping already uploaded window", continue.
   - Otherwise → evaluate rules → write block (new ULID) → upload → mark W_i as covered.
```

**Why this is robust:**
- `block.Upload` writes `meta.json` last. A crash at any prior point leaves no `meta.json`.
- Both Store GW and compactor skip ULID directories without `meta.json` — partial blocks
  are invisible to the system, not just to the backfill tool.
- The scan only reads `meta.json` files; it never deletes directories. This is safe in
  shared buckets where other components may be mid-upload.
- Incomplete windows are re-processed with a new ULID. The orphaned directory from the
  prior interrupted attempt is harmless and will be cleaned up by the compactor's
  `BlocksCleaner` or manual bucket maintenance.
- Cost: O(N) `meta.json` reads. For 6 months at 2h blocks: ~2,190 reads of ~1KB each —
  completes in seconds even against remote object storage.

**Concurrent job safety:** Two concurrent jobs with the same external labels may write the
same window simultaneously, landing two overlapping blocks in the backfill group. Without
vertical compaction this halts the compactor. Document this as unsupported; use unique
`backfill_job` values for concurrent runs.

---

## Appendix: Reference Code Pointers

### Promtool (source of algorithmic approach)

- **Rule loading and evaluation loop:** `ruleImporter.importAll` and `ruleImporter.importRule` in `/home/pvlltvk/Work/github/prometheus/cmd/promtool/rules.go` lines 84-189. This is the core loop to adapt.
- **Block duration alignment:** `getCompatibleBlockDuration` in `/home/pvlltvk/Work/github/prometheus/cmd/promtool/backfill.go` lines 71-85. Reuse this logic directly.
- **Memory-bounded appender:** `multipleAppender` in `/home/pvlltvk/Work/github/prometheus/cmd/promtool/rules.go` lines 191-239. Port this pattern.
- **CLI flag registration:** `/home/pvlltvk/Work/github/prometheus/cmd/promtool/main.go` lines 290-302.

### Thanos (integration points)

- **CLI registration pattern:** `registerCheckRules` in `/home/pvlltvk/Work/github/pvlltvk/thanos/cmd/thanos/tools.go` lines 38-47. Follow this `cmd.Setup(func(g *run.Group, ...) error { ... })` pattern.
- **Object storage client creation:** `registerBucketUploadBlocks` in `/home/pvlltvk/Work/github/pvlltvk/thanos/cmd/thanos/tools_bucket.go` lines 1476-1534. Pattern for `client.NewBucket`, wrapping with tracing and metrics.
- **Block upload:** `block.Upload` in `/home/pvlltvk/Work/github/pvlltvk/thanos/pkg/block/block.go` line 97.
- **Block metadata:** `metadata.Meta` and `metadata.Thanos` in `/home/pvlltvk/Work/github/pvlltvk/thanos/pkg/block/metadata/meta.go` lines 66-97.
- **Source types:** `/home/pvlltvk/Work/github/pvlltvk/thanos/pkg/block/metadata/meta.go` lines 34-48.
- **Query client:** `promclient.Client.QueryRange` in `/home/pvlltvk/Work/github/pvlltvk/thanos/pkg/promclient/promclient.go` line 535.
- **Time flag pattern:** `model.TimeOrDuration` in `/home/pvlltvk/Work/github/pvlltvk/thanos/pkg/model/timeduration.go` line 77. Used by bucket replicate for `--min-time`/`--max-time`.
- **Object store flag registration:** `extkingpin.RegisterCommonObjStoreFlags` used throughout `tools_bucket.go`.
- **Component registry:** `/home/pvlltvk/Work/github/pvlltvk/thanos/pkg/component/component.go` line 75+.

---

## Appendix B: Go Technical Review (golang-pro)

### Architecture Decision: HTTP PromQL API over gRPC Store API

**Recommendation: Use `promclient.Client.QueryRange()`** (HTTP PromQL API).

Reasons:
- Rule expressions are PromQL — they need a full PromQL engine to evaluate. The gRPC Store API only provides raw series data.
- Thanos Query already handles deduplication, partial response, and fan-out.
- The Thanos Rule component itself uses HTTP PromQL queries for evaluation.
- `promclient.Client` already handles JSON response parsing, warnings, and errors.

The gRPC Store API would only make sense for copying raw series (not evaluating expressions).

### Block Creation: Use `tsdb.NewBlockWriter` Directly

Use `tsdb.NewBlockWriter` from the Prometheus TSDB package, same as promtool. The Thanos `block.DiskWriter` is designed for lower-level series rewriting (compaction/rewrite) and lacks an `Appender` interface.

After `tsdb.BlockWriter.Flush()`, call `metadata.InjectThanos()` to write Thanos metadata, then `block.Upload()`.

### Proposed Package Layout

```go
// cmd/thanos/tools_rule_backfill.go — CLI registration
type ruleBackfillConfig struct {
    queryURL         *url.URL
    ruleFiles        []string
    start            time.Time
    end              time.Time
    evalInterval     time.Duration
    maxBlockDuration time.Duration
    outputDir        string
    externalLabels   []string
    concurrency      int
    dryRun           bool
}

func registerRuleBackfill(app extkingpin.AppClause) { ... }
```

```go
// pkg/rulebackfill/backfill.go — Core logic
package rulebackfill

// QueryRangeFunc abstracts the query API for testing.
type QueryRangeFunc func(ctx context.Context, query string,
    start, end time.Time, step time.Duration) (model.Matrix, []string, error)

type Backfiller struct {
    logger         log.Logger
    queryFunc      QueryRangeFunc
    outputDir      string
    externalLabels labels.Labels
    maxBlockDur    time.Duration
    maxSamples     int
    concurrency    int
}

func New(logger log.Logger, opts ...Option) *Backfiller { ... }
func (b *Backfiller) Run(ctx context.Context, ruleFiles []string,
    start, end time.Time, evalInterval time.Duration) ([]ulid.ULID, error) { ... }
```

### Concurrency Model

```
main goroutine
  └── for each rule group (sequential — rules may depend on each other)
       └── semaphore-bounded worker pool (--concurrency)
            └── for each block window
                 ├── QueryRange (HTTP, context-aware)
                 ├── BlockWriter.Append + Flush
                 ├── InjectThanos
                 └── block.Upload
```

Use `golang.org/x/sync/errgroup` with `SetLimit(concurrency)`. Rules within the same group MUST be evaluated sequentially if they reference each other. Independent groups can be parallelized.

### Memory Management

- Chunk time range to `maxBlockDuration` (default 2h). A 2h window at 1m step with 10k series ≈ 38MB — manageable.
- Adopt promtool's `multipleAppender` pattern with configurable `maxSamplesInMemory` (default 5000).
- Process rules sequentially within a group, groups in parallel up to `--concurrency`.
- Nil out matrix variables after writing each block for GC.

### Error Handling Patterns

Follow Thanos conventions:
- Use `github.com/pkg/errors` for wrapping
- Aggregate errors via `errutil.MultiError`
- Continue on non-fatal errors (single rule failure doesn't abort the run)
- Context cancellation aborts in-flight HTTP queries and block writes
- Clean up partial blocks on failure

```go
var merr errutil.MultiError
for _, rule := range group.Rules() {
    if err := b.processRule(ctx, rule, group, start, end); err != nil {
        level.Error(logger).Log("msg", "failed to process rule", "rule", rule.Name(), "err", err)
        merr.Add(errors.Wrapf(err, "rule %s", rule.Name()))
        continue
    }
}
return merr.Err()
```

### Dependencies (All Already Available in Thanos)

- `github.com/prometheus/prometheus/tsdb` — BlockWriter
- `github.com/prometheus/prometheus/rules` — Manager.LoadGroups()
- `github.com/prometheus/common/model` — model.Matrix
- `github.com/thanos-io/objstore` — bucket interface
- `github.com/thanos-io/thanos/pkg/promclient` — QueryRange
- `github.com/thanos-io/thanos/pkg/block` — Upload, metadata.InjectThanos
- `golang.org/x/sync/errgroup` — bounded concurrency

### Testing Strategy

- **Unit tests:** Table-driven tests with mock `QueryRangeFunc` returning canned `model.Matrix` data
- **Integration tests:** Use `objstore.NewInMemBucket()` for upload tests; verify blocks are readable by `tsdb.OpenDBReadOnly`
- **E2E tests:** Thanos Query + Store GW + Minio; verify backfilled metrics are queryable

---

## Appendix C: SRE / Reliability Review (sre-engineer)

### Hard Requirements for Production Safety

These are non-negotiable for the feature to be safe in production. All items below are
included in Phase 1 (see Section 7):

1. **Label strategy:** Backfilled blocks MUST carry at least one external label (hard-error if missing, since `block.Upload` rejects empty labels). Blocks SHOULD carry at least one distinguishing external label (e.g., `backfill_job="<unique-value>"`) to place them in a separate compaction group. Use a unique value per job to prevent intra-backfill group overlaps across repeated runs. If the user intentionally uses the same labels as live data (Strategy A), `--compact.enable-vertical-compaction` MUST be active on the compactor before the first block is uploaded — without it, the compactor halts for the entire bucket on the next run. The tool warns at startup if live-data blocks are detected in the target range.
2. **Idempotent uploads via bucket scan:** On startup, the tool scans the bucket to build a set of already-completed windows (meta.json present, labels match, source is `rules.backfill`, time range in target). Completed windows are skipped. Incomplete windows are re-processed with a new ULID. Orphaned partial directories from prior interrupted runs are NOT deleted (to avoid racing with other components sharing the bucket); they are harmless and cleaned up by the compactor's `BlocksCleaner`. No local checkpoint file is needed — the bucket is the checkpoint. This handles container restarts, OOM kills, and network interruptions transparently.
3. **Rate limiting on Thanos Query:** Non-negotiable. Without it, a large backfill degrades query availability for production users.
4. **Bounded memory via 2h block windows:** Blocks must be written in bounded time windows and memory released between blocks.
5. **Rollback mechanism:** Manifest file listing uploaded ULIDs must ship with the feature (Phase 1). A dedicated rollback subcommand that writes deletion marks is Phase 3.
6. **Dry-run mode:** Must be implemented before any production use (Phase 1).

### Operational Risks (Detailed)

| Risk | Severity | Mitigation |
|---|---|---|
| Overlapping blocks trigger compactor halt | High | Distinct external labels create separate compaction group |
| Crash mid-upload leaves orphaned chunks in bucket | High | `meta.json` uploaded last (existing pattern); bucket scan tracks completed windows; orphaned directories are not deleted (safe in shared buckets) and are cleaned up by compactor's `BlocksCleaner` |
| Query overload from thousands of range queries | High | Token bucket rate limiter (default 10 QPS), configurable concurrency, backoff on 429/503 |
| OOM on large time ranges | High | Write blocks in 2h chunks, release memory between blocks, fail fast if series count exceeds threshold |
| Duplicate blocks on re-run | Medium | Bucket-scan-based resumption (completed windows detected by meta.json with matching labels and source); optionally check bucket for existing overlapping blocks |
| Rollback complexity | Medium | Manifest file listing all uploaded ULIDs + rollback subcommand writing deletion marks |

### Rate Limiting Defaults

- Default: 10 queries/second toward Thanos Query
- `--query-concurrency`: max parallel in-flight queries (default 1, up to 4)
- `--query-timeout`: per-query timeout (default 2 minutes)
- Exponential backoff on HTTP 429, 503, and 5xx responses (max 5 retries)

### Monitoring Metrics

The tool should expose a Prometheus `/metrics` endpoint:

```
backfill_blocks_total{status="success|failed|skipped"}
backfill_evaluations_total{status="success|failed"}
backfill_evaluations_duration_seconds (histogram)
backfill_current_timestamp_unix_seconds (gauge — for stall detection)
backfill_bytes_uploaded_total (counter)
backfill_query_retries_total
backfill_estimated_completion_seconds (gauge)
```

Key alert: `(time() - backfill_current_timestamp_unix_seconds) > 600` = stalled backfill.

### Block Validation Before Upload

Before uploading each block:
1. Open with `tsdb.OpenBlock()` and validate index
2. Check MinTime/MaxTime match expected values
3. Verify series count is non-zero (skip empty blocks)
4. Use existing `block.VerifyIndex` functionality

### Deployment Pattern

**One-off Kubernetes Job** (not CronJob). Recommended spec:
- Minimum: 2 CPU, 4Gi memory (small backfills); 8Gi+ for >10k series over >30 days
- `terminationGracePeriodSeconds: 300` for checkpoint flush
- Node affinity away from query/store gateway nodes

### Resource Estimation Formula

```
memory_bytes = num_series × avg_samples_per_series × 16 bytes × 2 (safety factor)
```

For 10k series with 2h blocks at 1m step: `10000 × 120 × 16 × 2 ≈ 38 MB` per block.

### Store Gateway Impact

New blocks trigger sync (default every 3 minutes). For >100 blocks, upload in batches of 20-30 with 5-minute waits to avoid overwhelming Store GW sync.

### Compactor Impact

If backfill produces blocks aligned to compaction level boundaries (2h aligned to clock hours), the compactor merges them progressively without special handling. Misaligned blocks are handled by `splitByRange` in the planner.

### Rollback Strategy

```bash
thanos tools backfill rollback \
  --manifest gs://bucket/backfill-manifests/job-20240301.json \
  --objstore.config-file objstore.yaml
```

Writes `deletion-mark.json` for each listed ULID. Compactor's `BlocksCleaner` deletes actual data after `--delete-delay` (default 48h).

### Post-Backfill Verification Checklist

1. Check upload metrics for expected block count
2. Run `thanos tools bucket ls --output json | jq 'select(.thanos.source == "rule.backfill")'`
3. Execute PromQL query covering backfilled time range — verify non-zero results
4. Check compactor logs for halt errors
5. Monitor `thanos_compact_group_compactions_failures_total` for 1 hour post-backfill

---

## 9. Implementation Review Follow-up

The implementation has now been reviewed against the code in `feature/rules-backfill`.

The review findings and the recommended remediation plan are maintained in:

- [Implementation Review Follow-up](./rules-backfill-implementation-review.md)

This follow-up should be treated as the source of truth for code-level fixes that must
be completed before the feature is considered production-safe.
