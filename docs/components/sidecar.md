# Sidecar

The `thanos sidecar` command runs a component that gets deployed along with a Prometheus instance. This allows sidecar to optionally upload metrics to object storage and allow [Queriers](query.md) to query Prometheus data with common, efficient StoreAPI.

In details:

* It implements Thanos' Store API on top of Prometheus' remote-read API. This allows [Queriers](query.md) to treat Prometheus servers as yet another source of time series data without directly talking to its APIs.
* Optionally, the sidecar uploads TSDB blocks to an object storage bucket as Prometheus produces them every 2 hours. This allows Prometheus servers to be run with relatively low retention while their historic data is made durable and queryable via object storage.

  NOTE: This still does NOT mean that Prometheus can be fully stateless, because if it crashes and restarts you will lose ~2 hours of metrics, so persistent disk for Prometheus is highly recommended. The closest to stateless you can get is using remote write (which Thanos supports, see [Receiver](receive.md). Remote write has other risks and consequences, and still if crashed you lose in positive case seconds of metrics data, so persistent disk is recommended in all cases.

* Optionally Thanos sidecar is able to watch Prometheus rules and configuration, decompress and substitute environment variables if needed and ping Prometheus to reload them. Read more about this in [here](#reloader-configuration)

Prometheus servers connected to the Thanos cluster via the sidecar are subject to a few limitations and recommendations for safe operations:

* The recommended Prometheus version is 2.2.1 or greater (including newest releases). This is due to Prometheus instability in previous versions as well as lack of `flags` endpoint.
* (!) The Prometheus `external_labels` section of the Prometheus configuration file has unique labels in the overall Thanos system. Those external labels will be used by the sidecar and then Thanos in many places. See [external labels](../storage.md#external-labels) docs.
* The `--web.enable-admin-api` flag is enabled to support sidecar to get metadata from Prometheus like external labels.
* The `--web.enable-lifecycle` flag is enabled if you want to use sidecar reloading features (`--reload.*` flags).

If you choose to use the sidecar to also upload data to object storage:

* Must specify object storage (`--objstore.*` flags)
* It only uploads uncompacted Prometheus blocks. For compacted blocks, see [Upload compacted blocks](#upload-compacted-blocks).
* The `--storage.tsdb.min-block-duration` and `--storage.tsdb.max-block-duration` must be set to equal values to disable local compaction in order to use Thanos sidecar upload, otherwise leave local compaction on if sidecar just exposes StoreAPI and your retention is normal. The default of `2h` is recommended. Mentioned parameters set to equal values disable the internal Prometheus compaction, which is needed to avoid the corruption of uploaded data when Thanos compactor does its job, this is critical for data consistency and should not be ignored if you plan to use Thanos compactor. Even though you set mentioned parameters equal, you might observe Prometheus internal metric `prometheus_tsdb_compactions_total` being incremented, don't be confused by that: Prometheus writes initial head block to filesystem via its internal compaction mechanism, but if you have followed recommendations - data won't be modified by Prometheus before the sidecar uploads it. Thanos sidecar will also check sanity of the flags set to Prometheus on the startup and log errors or warning if they have been configured improperly (#838).
* The retention of Prometheus is recommended to not be lower than three times of the min block duration, so 6 hours. This achieves resilience in the face of connectivity issues to the object storage since all local data will remain available within the Thanos cluster. If connectivity gets restored the backlog of blocks gets uploaded to the object storage.

## StoreAPI request limits

Use `--store.limits.request-series` and `--store.limits.request-samples` to limit individual StoreAPI Series requests. Both default to `0`, which disables the limit. The sample limit uses an estimate of 120 samples per chunk, including histogram chunks. Series split across multiple remote-read responses are counted once per response.

Sidecar checks these limits before retaining responses for sorting. If a limit is exceeded, the request fails without sending the buffered series. For streamed remote reads, Sidecar stops reading and closes the upstream response body. Rejected requests increment `thanos_store_selects_dropped_total` with reason `series` or `chunks`.

These limits reduce retained results for rejected requests, but do not impose a hard memory limit. Successful and unlimited requests still buffer their results for sorting. A decoded frame or a large chunk can consume memory before the check. Metadata rows are parsed incrementally, but the full response is validated before returning a limit failure. Sampled fallback responses are fully decoded before their series are checked. Concurrent requests have separate budgets.

## Series sorting memory

Sidecar retains final labels and raw chunks for sorting, then builds outgoing StoreAPI chunks as it sends each series. Metadata is parsed one row at a time. These changes reduce intermediate allocations and retained descriptors while preserving complete sorted results; chunk payloads can still dominate memory for large queries.

To move large sorted results to disk, set `--store.series-sort-spill-threshold` to a byte size of at least `1MB` and `--store.series-sort-spill-dir` to an existing writable directory. The default threshold is `0`, which disables disk sorting. Sidecar refuses to start with a smaller positive threshold, which would turn almost every series into its own temporary file. Requests below the threshold create no temporary files. For example:

```bash
thanos sidecar \
  --store.series-sort-spill-threshold=64MB \
  --store.series-sort-spill-dir=/var/lib/thanos/series-sort \
  --store.series-sort-max-spill-size=4GB
```

Sizes require a unit suffix, such as `64MB` or `4GB`, except for `0`. Directory access is checked lazily when the first run is spilled; a missing or unwritable directory fails that request.

The threshold applies to each request's retained sorting records, including labels, chunk descriptors and payloads. It is a buffering target, not a heap or RSS limit: the current decoded frame, the largest record, output batches, merge buffers and runtime overhead need additional memory. Concurrent requests have separate targets. Disk I/O also uses the operating system's page cache and can increase container memory usage.

`--store.series-sort-max-spill-size` limits each request's live temporary file bytes, including merge input and output files held at the same time. Its default `0` leaves disk usage unlimited. Use a dedicated directory on a filesystem with an appropriate quota for a process-wide disk ceiling. Budget exhaustion or an I/O error fails the request. Upstream and count-limit failures send no buffered series; errors while merging or sending can occur after some series have been sent.

When disk sorting is enabled, `prometheus_store_series_sort_spilled_requests_total` counts requests that spilled at least one run and `prometheus_store_series_sort_spilled_bytes_total` counts bytes written to temporary files, including merge outputs. `prometheus_store_series_sort_failed_requests_total` counts requests that failed while sorting on disk, with reason `spill_size` for an exhausted `--store.series-sort-max-spill-size` budget and `io` for other failures, including cleanup failures. Requests cancelled by the client are not counted as failures.

Disk sorting uses private request directories and a bounded merge fan-in. Files are removed on normal completion, errors and cancellation. A process crash, OOM kill, SIGKILL or cleanup I/O failure can leave request directories behind. Cleanup failures are returned with the request error. After stopping all Sidecars using the directory, remove only known stale `thanos-series-sort-*` request directories; Sidecar does not automatically sweep a shared directory.

Measure memory and latency with representative queries before enabling disk sorting. Sorting writes and reads temporary data, so completion and time to first response can increase with disk load, payload size and concurrent requests. This option remains disabled by default.

## Experimental streaming series sort

The hidden flag `--store.series-sort-streaming` makes Sidecar sort streamed remote read responses while forwarding them: a series is sent as soon as no later series can sort before it, so only a few series are held back instead of the whole response. It applies to streamed responses only, which then no longer use disk sorting; sampled fallback responses and metadata requests are buffered as before.

This only works when Prometheus returns the series sorted by their own labels, and when no series has its own, different value for an external label or an own label named like a replica label removed for the request. A request that breaks one of these rules fails with `FailedPrecondition`, possibly after part of the result has been sent.

`prometheus_store_series_sort_unstreamable_total` counts such requests with reason `unsorted`, `external_label` or `replica_label`. It is also maintained while the flag is off, when these requests still succeed and Sidecar logs an example at most once per minute, so check that it stays at zero before enabling streaming sort.

## Reloader Configuration

Thanos can watch changes in Prometheus configuration and refresh Prometheus configuration if `--web.enable-lifecycle` enabled.

You can configure watching for changes in directory via `--reloader.rule-dir=DIR_NAME` flag.

Thanos sidecar can watch `--reloader.config-file=CONFIG_FILE` configuration file, replace environment variables found in there in `$(VARIABLE)` format, and produce generated config in `--reloader.config-envsubst-file=OUT_CONFIG_FILE` file.

## Example basic deployment

```bash
prometheus \
  --storage.tsdb.max-block-duration=2h \
  --storage.tsdb.min-block-duration=2h \
  --web.enable-lifecycle
```

```bash
thanos sidecar \
    --tsdb.path        "/path/to/prometheus/data/dir" \
    --prometheus.url   "http://localhost:9090" \
    --objstore.config-file  "bucket.yml"
```

The example content of `bucket.yml`:

```yaml mdox-exec="go run scripts/cfggen/main.go --name=gcs.Config"
type: GCS
config:
  bucket: ""
  service_account: ""
  use_grpc: false
  grpc_conn_pool_size: 0
  http_config:
    idle_conn_timeout: 1m30s
    response_header_timeout: 2m
    insecure_skip_verify: false
    tls_handshake_timeout: 10s
    expect_continue_timeout: 1s
    max_idle_conns: 100
    max_idle_conns_per_host: 100
    max_conns_per_host: 0
    tls_config:
      ca_file: ""
      cert_file: ""
      key_file: ""
      server_name: ""
      insecure_skip_verify: false
    disable_compression: false
  chunk_size_bytes: 0
  max_retries: 0
prefix: ""
```

## Upload compacted blocks

If you want to migrate from a pure Prometheus setup to Thanos and have to keep the historical data, you can use the flag `--shipper.upload-compacted`. This will also upload blocks that were compacted by Prometheus. Values greater than 1 in the `compaction.level` field of a Prometheus block’s `meta.json` file indicate level of compaction.

To use this, the Prometheus compaction needs to be disabled. This can be done by setting the following flags for Prometheus:

- `--storage.tsdb.min-block-duration=2h`
- `--storage.tsdb.max-block-duration=2h`

## Flags

```$ mdox-exec="thanos sidecar --help"
usage: thanos sidecar [<flags>]

Sidecar for Prometheus server.


Flags:
  -h, --[no-]help                Show context-sensitive help (also try
                                 --help-long and --help-man).
      --[no-]version             Show application version.
      --log.level=info           Log filtering level.
      --log.format=logfmt        Log format to use. Possible options: logfmt,
                                 json or journald.
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag
                                 (mutually exclusive). Content of YAML file
                                 with tracing configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --[no-]enable-auto-gomemlimit
                                 Enable go runtime to automatically limit memory
                                 consumption.
      --auto-gomemlimit.ratio=0.9
                                 The ratio of reserved GOMEMLIMIT memory to the
                                 detected maximum container or system memory.
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --http.config=""           [EXPERIMENTAL] Path to the configuration file
                                 that can enable TLS or authentication for all
                                 HTTP endpoints.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components.
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no
                                 client CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --grpc-server-tls-min-version=1.3
                                 TLS supported minimum version for gRPC server.
                                 If no version is specified, it'll default to
                                 1.3. Allowed values: ["1.0", "1.1", "1.2",
                                 "1.3"]
      --grpc-server-tls-ciphers=GRPC-SERVER-TLS-CIPHERS ...
                                 TLS cipher suites for gRPC server
                                 (repeatable). If not specified,
                                 the default Go cipher suites are used.
                                 See https://pkg.go.dev/crypto/tls#pkg-constants
                                 for valid values.
      --grpc-server-tls-curves=GRPC-SERVER-TLS-CURVES ...
                                 TLS curves for gRPC server (repeatable). If
                                 not specified, the default Go curves are used.
                                 Valid values: CurveP256, CurveP384, CurveP521,
                                 X25519.
      --grpc-server-max-connection-age=60m
                                 The grpc server max connection age. This
                                 controls how often to re-establish connections
                                 and redo TLS handshakes.
      --grpc-grace-period=2m     Time to wait after an interrupt received for
                                 GRPC Server.
      --prometheus.url=http://localhost:9090
                                 URL at which to reach Prometheus's API.
                                 For better performance use local network.
      --prometheus.ready_timeout=10m
                                 Maximum time to wait for the Prometheus
                                 instance to start up
      --prometheus.get_config_interval=30s
                                 How often to get Prometheus config
      --prometheus.get_config_timeout=30s
                                 Timeout for getting Prometheus config
      --prometheus.http-client-file=<file-path>
                                 Path to YAML file or string with http
                                 client configs. See Format details:
                                 https://thanos.io/tip/components/sidecar.md/#configuration.
      --prometheus.http-client=<content>
                                 Alternative to 'prometheus.http-client-file'
                                 flag (mutually exclusive). Content
                                 of YAML file or string with http
                                 client configs. See Format details:
                                 https://thanos.io/tip/components/sidecar.md/#configuration.
      --tsdb.path="./data"       Data directory of TSDB.
      --reloader.config-file=""  Config file watched by the reloader.
      --reloader.config-envsubst-file=""
                                 Output file for environment variable
                                 substituted config file.
      --reloader.rule-dir=RELOADER.RULE-DIR ...
                                 Rule directories for the reloader to refresh
                                 (repeated field).
      --reloader.watch-interval=3m
                                 Controls how often reloader re-reads config and
                                 rules.
      --reloader.retry-interval=5s
                                 Controls how often reloader retries config
                                 reload in case of error.
      --reloader.method=http     Method used to reload the configuration.
      --reloader.process-name="prometheus"
                                 Executable name used to match the process being
                                 reloaded when using the signal method.
      --request.logging-config-file=<file-path>
                                 Path to YAML file with request logging
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/logging.md/#configuration
      --request.logging-config=<content>
                                 Alternative to 'request.logging-config-file'
                                 flag (mutually exclusive). Content
                                 of YAML file with request logging
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/logging.md/#configuration
      --objstore.config-file=<file-path>
                                 Path to YAML file that contains object
                                 store configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                                 Alternative to 'objstore.config-file'
                                 flag (mutually exclusive). Content of
                                 YAML file that contains object store
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --[no-]shipper.upload-compacted
                                 If true shipper will try to upload compacted
                                 blocks as well. Useful for migration purposes.
                                 Works only if compaction is disabled on
                                 Prometheus. Do it once and then disable the
                                 flag when done.
      --hash-func=               Specify which hash function to use when
                                 calculating the hashes of produced files.
                                 If no function has been specified, it does not
                                 happen. This permits avoiding downloading some
                                 files twice albeit at some performance cost.
                                 Possible values are: "", "SHA256".
      --shipper.meta-file-name="thanos.shipper.json"
                                 the file to store shipper metadata in
      --shipper.upload-concurrency=0
                                 Number of goroutines to use when uploading
                                 block files to object storage.
      --store.limits.request-series=0
                                 The maximum series allowed for a single Series
                                 request. The Series call fails if this limit is
                                 exceeded. 0 means no limit.
      --store.limits.request-samples=0
                                 The maximum samples allowed for a single
                                 Series request, The Series call fails if
                                 this limit is exceeded. 0 means no limit.
                                 NOTE: For efficiency the limit is internally
                                 implemented as 'chunks limit' considering each
                                 chunk contains a maximum of 120 samples.
      --store.series-sort-spill-threshold=0
                                 Per-request retained series sorting buffer
                                 target as a size with units, e.g. 64MB.
                                 0 disables disk sorting, other values must
                                 be at least 1MB. Frames, the largest record,
                                 output batches and merge buffers can exceed
                                 this target.
      --store.series-sort-spill-dir=""
                                 Directory for private per-request series
                                 sorting files. Required when disk sorting is
                                 enabled; use an existing writable directory.
      --store.series-sort-max-spill-size=0
                                 Maximum live temporary sorting file size per
                                 request, e.g. 4GB, including simultaneous merge
                                 inputs and outputs. 0 means unlimited.
      --min-time=0000-01-01T00:00:00Z
                                 Start of time range limit to serve. Thanos
                                 sidecar will serve only metrics, which happened
                                 later than this value. Option can be a constant
                                 time in RFC3339 format or time duration
                                 relative to current time, such as -1d or 2h45m.
                                 Valid duration units are ms, s, m, h, d, w, y.

```

## Configuration

### Prometheus HTTP client

You can configure the Prometheus HTTP client for Thanos sidecar with YAML, either by passing the YAML content directly to the `--prometheus.http-client` flag, or by passing the YAML file path to the `--prometheus.http-client-file` flag.

The configuration format is the following:

```yaml
basic_auth:
  username: ""
  password: ""
  password_file: ""
bearer_token: ""
bearer_token_file: ""
proxy_url: ""
tls_config:
  ca_file: ""
  cert_file: ""
  key_file: ""
  server_name: ""
  insecure_skip_verify: false
transport_config:
  max_idle_conns: 0
  max_idle_conns_per_host: 0
  idle_conn_timeout: 0
  response_header_timeout: 0
  expect_continue_timeout: 0
  max_conns_per_host: 0
  disable_compression: false
  tls_handshake_timeout: 0
```
