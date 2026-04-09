# Rules Backfill Implementation Review

**Date:** 2026-04-05
**Branch:** `feature/rules-backfill`
**Scope:** `cmd/thanos/tools_rules_backfill.go`, `pkg/rulesbackfill/*`
**Status:** Review findings and fix plan

---

## Summary

The implementation is close to the intended design, but there are four correctness issues
that should be fixed before the feature is considered safe to run against production data:

1. `tsdb.BlockWriter` is used with an unsafe block size, which can silently drop samples.
2. Per-rule query failures are converted into partial blocks that are later treated as complete.
3. The first backfill window can include data from before the user-requested start time.
4. Rule group intervals from the rule files are ignored, so evaluation timestamps can differ
   from Prometheus semantics.

The rest of this document records the findings and a concrete remediation plan.

---

## Findings

### 1. Unsafe BlockWriter usage can silently drop samples

**Severity:** High

**Affected code:**

- `pkg/rulesbackfill/backfill.go:278`
- `pkg/rulesbackfill/backfill.go:341`
- `pkg/rulesbackfill/backfill.go:353`

**Problem:**

The implementation creates the writer with:

```go
tsdb.NewBlockWriter(..., blockDurMs)
```

Promtool intentionally uses `2*blockDuration` when backfilling rules. The reason is that
`BlockWriter` rejects samples older than the appendable minimum time once the head advances.
With the current code, if one series appends a late timestamp first, then another series
appends an earlier timestamp from the same window, `Appender.Append` can return
`storage.ErrOutOfBounds`.

The code currently logs the append error and continues, which means the block may upload with
missing samples and no hard failure.

**Impact:**

- Silent partial data loss inside a block.
- The current tests do not catch this because they mostly use one series or one timestamp per rule.

**Fix direction:**

- Use the same approach as promtool: create the writer with `2*blockDurMs`.
- Keep writing only samples from the intended window; the doubled writer range is only there
  to satisfy TSDB append constraints.
- Treat append failures as fatal for the window unless they are explicitly expected and handled.

---

### 2. Partial rule failures produce blocks that are later considered complete

**Severity:** High

**Affected code:**

- `pkg/rulesbackfill/backfill.go:202`
- `pkg/rulesbackfill/backfill.go:308`
- `pkg/rulesbackfill/backfill.go:376`
- `pkg/rulesbackfill/backfill_test.go:440`

**Problem:**

If one rule in a window fails to query, the implementation logs the error and continues
building the block from the remaining rules. If at least one rule succeeds, the block is
still uploaded. On the next run, `ScanBucket` sees that block and marks the window as covered.

This makes the idempotency mechanism incorrect: a partially successful window is treated as
fully completed.

**Impact:**

- A rerun will skip windows that only contain a subset of the requested rule outputs.
- The operator gets incomplete historical data without a reliable recovery path.

**Fix direction:**

- Track whether any rule failed for the window.
- If any rule fails, do not upload the block for that window.
- Clean up the temporary local block directory for that failed window.
- Return a window-level error so the run summary and exit status reflect the failure.
- Update tests to expect a failure summary rather than a successful no-op.

**Optional enhancement:**

- If resumability for partially completed work is important, write a manifest or local state
  only after the full window succeeds, not per partial block.

---

### 3. First window may backfill data before the requested start time

**Severity:** High

**Affected code:**

- `pkg/rulesbackfill/backfill.go:161`
- `pkg/rulesbackfill/backfill.go:207`
- `pkg/rulesbackfill/backfill.go:308`
- `pkg/rulesbackfill/backfill.go:465`

**Problem:**

`computeWindows` aligns the first window down to the previous block boundary. That part is
fine for block planning. The issue is that the aligned boundary is then passed directly to
`queryFunc` as the actual query start.

If the user requests:

- `start = 2026-04-01T00:30:00Z`
- block size = `2h`

the first query will start at `2026-04-01T00:00:00Z`, not `00:30:00Z`.

Promtool does not do this. It clamps the query start per block to:

```go
max(blockStart, requestedStart)
```

and then aligns evaluation timestamps within that range.

**Impact:**

- The tool can backfill data outside the user-requested interval.
- The uploaded block metadata remains valid, but the contents violate the CLI contract.

**Fix direction:**

- Keep block windows aligned for block generation.
- Pass both the requested range and the current block window into `processWindow`.
- For each window, compute:
  - `queryStart = max(windowStart, requestedStart)`
  - `queryEnd = min(windowEndExclusive-1 or windowEnd, requestedEnd)` depending on the
    final query boundary contract you choose.
- Skip the window if the clamped interval is empty.

---

### 4. Rule file group intervals are ignored

**Severity:** Medium

**Affected code:**

- `cmd/thanos/tools_rules_backfill.go:76`
- `pkg/rulesbackfill/backfill.go:123`
- `pkg/rulesbackfill/backfill.go:242`
- `pkg/rulesbackfill/backfill.go:284`

**Problem:**

The implementation parses rule files with `rulefmt.ParseFile` and then evaluates every rule
using the single global `b.evalInterval`.

That is not how Prometheus or promtool behave. Rule files can define a per-group `interval`,
and promtool preserves group evaluation cadence via the loaded Prometheus rule groups.

**Impact:**

- Backfilled samples can land on different timestamps than live rule evaluation would have produced.
- Any rule with a group-specific interval is especially affected.

**Fix direction:**

- Preserve the group interval during parsing by extending `ruleGroup` with an `Interval` field.
- When a rule group defines `interval`, use it.
- When a rule group does not define `interval`, fall back to the CLI default `--eval-interval`.
- For full semantic parity, also align the first evaluation timestamp within each window using
  Prometheus group slotting logic rather than simply using the clamped window start.

---

## Recommended Fix Plan

### Phase 1: Correctness fixes

These changes should be done before any further UX work.

1. Fix BlockWriter usage in `pkg/rulesbackfill/backfill.go`.
   Change `tsdb.NewBlockWriter(..., blockDurMs)` to `tsdb.NewBlockWriter(..., 2*blockDurMs)`.
   Add a comment explaining why the larger writer range is required.

2. Make append failures fail the window.
   Replace the current `level.Error(...); continue` path around `app.Append` with window-level
   error collection. If any append fails, abort the window, rollback or discard the current
   block, and return an error.

3. Make query failures fail the window.
   Track rule failures in `processWindow`. If any query fails, stop before upload and return
   an error for that window. Do not mark the window as complete.

4. Clamp each query to the requested range.
   Keep aligned window planning, but compute a clamped query start/end inside `processWindow`.
   This needs small signature changes so `Run` passes both requested bounds and window bounds.

5. Preserve group intervals.
   Extend `ruleGroup` to include `Interval time.Duration`.
   Parse `rg.Interval` from the YAML, and use `group.Interval` when building the query step.
   If `rg.Interval == 0`, use `b.evalInterval`.

### Phase 2: Prometheus semantic alignment

After the correctness fixes above, make the backfill behavior closer to promtool.

1. Align the first evaluation timestamp inside each window.
   Prometheus does not necessarily evaluate exactly at the raw window start. It uses the group
   interval and group hash slotting. Reproduce the promtool behavior for:
   - first timestamp in a window
   - last timestamp in a window
   - empty windows after alignment

2. Revisit rule parsing.
   If practical, move from `rulefmt.ParseFile` plus custom structs to the same Prometheus rule
   loading path used by promtool, or at least copy the relevant interval and timestamp logic
   explicitly into this package.

3. Tighten idempotency semantics.
   A window should only be considered complete if all intended rules for that window succeeded.
   If you keep manifests, record per-window success so future resumability logic is explicit.

### Phase 3: Test coverage fixes

The current tests pass, but they do not protect the high-risk paths above.

Add these tests:

1. Multi-series append ordering test.
   Construct one query result where series A appends a late timestamp first and series B then
   appends an earlier timestamp from the same window. Verify the window succeeds with the
   doubled writer range and fails with the old behavior.

2. Partial rule failure test.
   Use two rules in one window, make one succeed and one fail. Verify:
   - no block is uploaded
   - the run returns an error
   - the window is not considered covered on rerun

3. Non-aligned start time test.
   Use `start` in the middle of a 2h block and verify the first query starts at the requested
   start, not the aligned block boundary.

4. Group interval test.
   Create two rule groups with different `interval` values. Verify the query step differs per
   group and the expected timestamps are used.

5. Recovery test after a failed window.
   First run fails one rule. Second run succeeds all rules. Verify the second run does not skip
   the previously failed window.

---

## Suggested Implementation Order

Use this sequence to minimize churn:

1. Fix `processWindow` to fail on query/append errors.
2. Change the writer size to `2*blockDurMs`.
3. Thread requested start/end through the window processing path and clamp queries.
4. Extend parsed rule groups to carry `interval`.
5. Add regression tests for all four findings.
6. Run:
   - `go test ./pkg/rulesbackfill/...`
   - `go test ./cmd/thanos/...`
   - any relevant end-to-end tests once available

---

## Acceptance Criteria For The Next Revision

The implementation can be considered ready for another review when all of the following are true:

1. A failed rule in a window prevents that window from being uploaded.
2. A rerun after a partial failure reprocesses the failed window instead of skipping it.
3. The first uploaded samples are never earlier than the user-requested `--start`.
4. Rule groups with custom `interval` values preserve that cadence during backfill.
5. There is an automated regression test for each item above.
