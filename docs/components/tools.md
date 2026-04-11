# Tools

The `thanos tools` subcommand of Thanos is a set of additional CLI, short-living tools that are meant to be ran for development or debugging purposes.

All commands added as tools should land in `tools.go` or file with `tools_` prefix.

## Flags

```$ mdox-exec="thanos tools --help"
usage: thanos tools <command> [<args> ...]

Tools utility commands


Flags:
  -h, --[no-]help          Show context-sensitive help (also try --help-long and
                           --help-man).
      --[no-]version       Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt, json or
                           journald.
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

Subcommands:
tools bucket verify [<flags>]
    Verify all blocks in the bucket against specified issues. NOTE: Depending on
    issue this might take time and will need downloading all specified blocks to
    disk.

tools bucket ls [<flags>]
    List all blocks in the bucket.

tools bucket inspect [<flags>]
    Inspect all blocks in the bucket in detailed, table-like way.

tools bucket web [<flags>]
    Web interface for remote storage bucket.

tools bucket replicate [<flags>]
    Replicate data from one object storage to another. NOTE: Currently it works
    only with Thanos blocks (meta.json has to have Thanos metadata).

tools bucket downsample [<flags>]
    Continuously downsamples blocks in an object store bucket.

tools bucket cleanup [<flags>]
    Cleans up all blocks marked for deletion.

tools bucket mark --id=ID --marker=MARKER [<flags>]
    Mark block for deletion or no-compact in a safe way. NOTE: If the compactor
    is currently running compacting same block, this operation would be
    potentially a noop.

tools bucket rewrite --id=ID [<flags>]
    Rewrite chosen blocks in the bucket, while deleting or modifying
    series Resulted block has modified stats in meta.json. Additionally
    compaction.sources are altered to not confuse readers of meta.json.
    Instead thanos.rewrite section is added with useful info like old sources
    and deletion requests. NOTE: It's recommended to turn off compactor while
    doing this operation. If the compactor is running and touching exactly same
    block that is being rewritten, the resulted rewritten block might only cause
    overlap (mitigated by marking overlapping block manually for deletion) and
    the data you wanted to rewrite could already part of bigger block.

    Use FILESYSTEM type of bucket to rewrite block on disk (suitable for vanilla
    Prometheus) After rewrite, it's caller responsibility to delete or mark
    source block for deletion to avoid overlaps. WARNING: This procedure is
    *IRREVERSIBLE* after certain time (delete delay), so do backup your blocks
    first.

tools bucket retention [<flags>]
    Retention applies retention policies on the given bucket. Please make sure
    no compactor is running on the same bucket at the same time.

tools bucket upload-blocks [<flags>]
    Upload blocks push blocks from the provided path to the object storage.

tools rules-check --rules=RULES
    Check if the rule files are valid or not.


```

## Bucket

The `thanos tools bucket` subcommand of Thanos is a set of commands to inspect data in object storage buckets. It is normally run as a standalone command to aid with troubleshooting.

Example:

```bash
thanos tools bucket verify --objstore.config-file=bucket.yml
```

The content of `bucket.yml`:

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

Bucket can be extended to add more subcommands that will be helpful when working with object storage buckets by adding a new command within [`/cmd/thanos/tools_bucket.go`](../../cmd/thanos/tools_bucket.go)  .

```$ mdox-exec="thanos tools bucket --help"
usage: thanos tools bucket [<flags>] <command> [<args> ...]

Bucket utility commands


Flags:
  -h, --[no-]help          Show context-sensitive help (also try --help-long and
                           --help-man).
      --[no-]version       Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt, json or
                           journald.
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
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (mutually
                           exclusive). Content of YAML file that contains
                           object store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration

Subcommands:
tools bucket verify [<flags>]
    Verify all blocks in the bucket against specified issues. NOTE: Depending on
    issue this might take time and will need downloading all specified blocks to
    disk.

tools bucket ls [<flags>]
    List all blocks in the bucket.

tools bucket inspect [<flags>]
    Inspect all blocks in the bucket in detailed, table-like way.

tools bucket web [<flags>]
    Web interface for remote storage bucket.

tools bucket replicate [<flags>]
    Replicate data from one object storage to another. NOTE: Currently it works
    only with Thanos blocks (meta.json has to have Thanos metadata).

tools bucket downsample [<flags>]
    Continuously downsamples blocks in an object store bucket.

tools bucket cleanup [<flags>]
    Cleans up all blocks marked for deletion.

tools bucket mark --id=ID --marker=MARKER [<flags>]
    Mark block for deletion or no-compact in a safe way. NOTE: If the compactor
    is currently running compacting same block, this operation would be
    potentially a noop.

tools bucket rewrite --id=ID [<flags>]
    Rewrite chosen blocks in the bucket, while deleting or modifying
    series Resulted block has modified stats in meta.json. Additionally
    compaction.sources are altered to not confuse readers of meta.json.
    Instead thanos.rewrite section is added with useful info like old sources
    and deletion requests. NOTE: It's recommended to turn off compactor while
    doing this operation. If the compactor is running and touching exactly same
    block that is being rewritten, the resulted rewritten block might only cause
    overlap (mitigated by marking overlapping block manually for deletion) and
    the data you wanted to rewrite could already part of bigger block.

    Use FILESYSTEM type of bucket to rewrite block on disk (suitable for vanilla
    Prometheus) After rewrite, it's caller responsibility to delete or mark
    source block for deletion to avoid overlaps. WARNING: This procedure is
    *IRREVERSIBLE* after certain time (delete delay), so do backup your blocks
    first.

tools bucket retention [<flags>]
    Retention applies retention policies on the given bucket. Please make sure
    no compactor is running on the same bucket at the same time.

tools bucket upload-blocks [<flags>]
    Upload blocks push blocks from the provided path to the object storage.


```

### Bucket Web

`tools bucket web` is used to inspect bucket blocks in form of interactive web UI.

This will start local webserver that will periodically update the view with given refresh.

<img src="../img/bucket-web.jpg" class="img-fluid" alt="web"/>

Example:

```
thanos tools bucket web --objstore.config-file="..."
```

```$ mdox-exec="thanos tools bucket web --help"
usage: thanos tools bucket web [<flags>]

Web interface for remote storage bucket.


Flags:
  -h, --[no-]help               Show context-sensitive help (also try
                                --help-long and --help-man).
      --[no-]version            Show application version.
      --log.level=info          Log filtering level.
      --log.format=logfmt       Log format to use. Possible options: logfmt,
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
      --http-address="0.0.0.0:10902"
                                Listen host:port for HTTP endpoints.
      --http-grace-period=2m    Time to wait after an interrupt received for
                                HTTP Server.
      --http.config=""          [EXPERIMENTAL] Path to the configuration file
                                that can enable TLS or authentication for all
                                HTTP endpoints.
      --web.route-prefix=""     Prefix for API and UI endpoints. This allows
                                thanos UI to be served on a sub-path.
                                Defaults to the value of --web.external-prefix.
                                This option is analogous to --web.route-prefix
                                of Prometheus.
      --web.external-prefix=""  Static prefix for all HTML links and redirect
                                URLs in the bucket web UI interface.
                                Actual endpoints are still served on / or the
                                web.route-prefix. This allows thanos bucket
                                web UI to be served behind a reverse proxy that
                                strips a URL sub-path.
      --web.prefix-header=""    Name of HTTP request header used for dynamic
                                prefixing of UI links and redirects.
                                This option is ignored if web.external-prefix
                                argument is set. Security risk: enable
                                this option only if a reverse proxy in
                                front of thanos is resetting the header.
                                The --web.prefix-header=X-Forwarded-Prefix
                                option can be useful, for example, if Thanos
                                UI is served via Traefik reverse proxy with
                                PathPrefixStrip option enabled, which sends the
                                stripped prefix value in X-Forwarded-Prefix
                                header. This allows thanos UI to be served on a
                                sub-path.
      --[no-]web.disable-cors   Whether to disable CORS headers to be set by
                                Thanos. By default Thanos sets CORS headers to
                                be allowed by all.
      --refresh=30m             Refresh interval to download metadata from
                                remote storage
      --timeout=5m              Timeout to download metadata from remote storage
      --label=LABEL             External block label to use as group title
      --[no-]disable-admin-operations
                                Disable UI/API admin operations like marking
                                blocks for deletion and no compaction.
      --min-time=0000-01-01T00:00:00Z
                                Start of time range limit to serve. Thanos
                                tool bucket web will serve only blocks, which
                                happened later than this value. Option can be a
                                constant time in RFC3339 format or time duration
                                relative to current time, such as -1d or 2h45m.
                                Valid duration units are ms, s, m, h, d, w, y.
      --max-time=9999-12-31T23:59:59Z
                                End of time range limit to serve. Thanos
                                tool bucket web will serve only blocks,
                                which happened earlier than this value. Option
                                can be a constant time in RFC3339 format or time
                                duration relative to current time, such as -1d
                                or 2h45m. Valid duration units are ms, s, m, h,
                                d, w, y.
      --selector.relabel-config-file=<file-path>
                                Path to YAML file with relabeling
                                configuration that allows selecting blocks
                                to act on based on their external labels.
                                It follows thanos sharding relabel-config
                                syntax. For format details see:
                                https://thanos.io/tip/thanos/sharding.md/#relabelling
      --selector.relabel-config=<content>
                                Alternative to 'selector.relabel-config-file'
                                flag (mutually exclusive). Content of YAML
                                file with relabeling configuration that allows
                                selecting blocks to act on based on their
                                external labels. It follows thanos sharding
                                relabel-config syntax. For format details see:
                                https://thanos.io/tip/thanos/sharding.md/#relabelling

```

### Bucket Verify

`tools bucket verify` is used to verify and optionally repair blocks within the specified bucket.

Example:

```
thanos tools bucket verify --objstore.config-file="..."
```

When using the `--repair` option, make sure that the compactor job is disabled first.

```$ mdox-exec="thanos tools bucket verify --help"
usage: thanos tools bucket verify [<flags>]

Verify all blocks in the bucket against specified issues. NOTE: Depending on
issue this might take time and will need downloading all specified blocks to
disk.


Flags:
  -h, --[no-]help          Show context-sensitive help (also try --help-long and
                           --help-man).
      --[no-]version       Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt, json or
                           journald.
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
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (mutually
                           exclusive). Content of YAML file that contains
                           object store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore-backup.config-file=<file-path>
                           Path to YAML file that contains object
                           store-backup configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
                           Used for repair logic to backup blocks before
                           removal.
      --objstore-backup.config=<content>
                           Alternative to 'objstore-backup.config-file'
                           flag (mutually exclusive). Content of YAML
                           file that contains object store-backup
                           configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
                           Used for repair logic to backup blocks before
                           removal.
  -r, --[no-]repair        Attempt to repair blocks for which issues were
                           detected
  -i, --issues=index_known_issues... ...
                           Issues to verify (and optionally repair). Possible
                           issue to verify, without repair: [overlapped_blocks];
                           Possible issue to verify and repair:
                           [index_known_issues duplicated_compaction]
      --id=ID ...          Block IDs to verify (and optionally repair) only.
                           If none is specified, all blocks will be verified.
                           Repeated field
      --delete-delay=0s    Duration after which blocks marked for deletion
                           would be deleted permanently from source bucket by
                           compactor component. If delete-delay is non zero,
                           blocks will be marked for deletion and compactor
                           component is required to delete blocks from source
                           bucket. If delete-delay is 0, blocks will be deleted
                           straight away. Use this if you want to get rid of
                           or move the block immediately. Note that deleting
                           blocks immediately can cause query failures, if store
                           gateway still has the block loaded, or compactor is
                           ignoring the deletion because it's compacting the
                           block at the same time.

```

### Bucket ls

`tools bucket ls` is used to list all blocks in the specified bucket.

Example:

```
thanos tools bucket ls -o json --objstore.config-file="..."
```

```$ mdox-exec="thanos tools bucket ls --help"
usage: thanos tools bucket ls [<flags>]

List all blocks in the bucket.


Flags:
  -h, --[no-]help            Show context-sensitive help (also try --help-long
                             and --help-man).
      --[no-]version         Show application version.
      --log.level=info       Log filtering level.
      --log.format=logfmt    Log format to use. Possible options: logfmt,
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
      --selector.relabel-config-file=<file-path>
                             Path to YAML file with relabeling configuration
                             that allows selecting blocks to act on based on
                             their external labels. It follows thanos sharding
                             relabel-config syntax. For format details see:
                             https://thanos.io/tip/thanos/sharding.md/#relabelling
      --selector.relabel-config=<content>
                             Alternative to 'selector.relabel-config-file'
                             flag (mutually exclusive). Content of YAML
                             file with relabeling configuration that allows
                             selecting blocks to act on based on their
                             external labels. It follows thanos sharding
                             relabel-config syntax. For format details see:
                             https://thanos.io/tip/thanos/sharding.md/#relabelling
  -o, --output=""            Optional format in which to print each block's
                             information. Options are 'json', 'wide' or a custom
                             template.
      --[no-]exclude-delete  Exclude blocks marked for deletion.
      --min-time=0000-01-01T00:00:00Z
                             Start of time range limit to list blocks. Thanos
                             Tools will list blocks, which were created later
                             than this value. Option can be a constant time in
                             RFC3339 format or time duration relative to current
                             time, such as -1d or 2h45m. Valid duration units
                             are ms, s, m, h, d, w, y.
      --max-time=9999-12-31T23:59:59Z
                             End of time range limit to list. Thanos Tools
                             will list only blocks, which were created earlier
                             than this value. Option can be a constant time in
                             RFC3339 format or time duration relative to current
                             time, such as -1d or 2h45m. Valid duration units
                             are ms, s, m, h, d, w, y.
      --timeout=5m           Timeout to download metadata from remote storage

```

### Bucket inspect

`tools bucket inspect` is used to inspect buckets in a detailed way using stdout in ASCII table format.

Example:

```
thanos tools bucket inspect -l environment=\"prod\" --objstore.config-file="..."
```

```$ mdox-exec="thanos tools bucket inspect --help"
usage: thanos tools bucket inspect [<flags>]

Inspect all blocks in the bucket in detailed, table-like way.


Flags:
  -h, --[no-]help            Show context-sensitive help (also try --help-long
                             and --help-man).
      --[no-]version         Show application version.
      --log.level=info       Log filtering level.
      --log.format=logfmt    Log format to use. Possible options: logfmt,
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
  -l, --selector=<name>=\"<value>\" ...
                             Selects blocks based on label, e.g. '-l
                             key1=\"value1\" -l key2=\"value2\"'. All key value
                             pairs must match.
      --sort-by=FROM... ...  Sort by columns. It's also possible to sort by
                             multiple columns, e.g. '--sort-by FROM --sort-by
                             UNTIL'. I.e., if the 'FROM' value is equal the rows
                             are then further sorted by the 'UNTIL' value.
      --timeout=5m           Timeout to download metadata from remote storage
      --output=table         Output format for result. Currently supports table,
                             csv, tsv.

```

### Bucket replicate

`bucket tools replicate` is used to replicate buckets from one object storage to another.

NOTE: Currently it works only with Thanos blocks (meta.json has to have Thanos metadata).

Example:

```
thanos tools bucket replicate --objstore.config-file="..." --objstore-to.config="..."
```

```$ mdox-exec="thanos tools bucket replicate --help"
usage: thanos tools bucket replicate [<flags>]

Replicate data from one object storage to another. NOTE: Currently it works only
with Thanos blocks (meta.json has to have Thanos metadata).


Flags:
  -h, --[no-]help             Show context-sensitive help (also try --help-long
                              and --help-man).
      --[no-]version          Show application version.
      --log.level=info        Log filtering level.
      --log.format=logfmt     Log format to use. Possible options: logfmt,
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
      --http-address="0.0.0.0:10902"
                              Listen host:port for HTTP endpoints.
      --http-grace-period=2m  Time to wait after an interrupt received for HTTP
                              Server.
      --http.config=""        [EXPERIMENTAL] Path to the configuration file
                              that can enable TLS or authentication for all HTTP
                              endpoints.
      --objstore-to.config-file=<file-path>
                              Path to YAML file that contains object
                              store-to configuration. See format details:
                              https://thanos.io/tip/thanos/storage.md/#configuration
                              The object storage which replicate data to.
      --objstore-to.config=<content>
                              Alternative to 'objstore-to.config-file'
                              flag (mutually exclusive). Content of
                              YAML file that contains object store-to
                              configuration. See format details:
                              https://thanos.io/tip/thanos/storage.md/#configuration
                              The object storage which replicate data to.
      --resolution=0s... ...  Only blocks with these resolutions will be
                              replicated. Repeated flag.
      --compaction-min=1      Only blocks with at least this compaction level
                              will be replicated.
      --compaction-max=4      Only blocks up to a maximum of this compaction
                              level will be replicated.
      --compaction=COMPACTION ...
                              Only blocks with these compaction levels
                              will be replicated. Repeated flag. Overrides
                              compaction-min and compaction-max if set.
      --matcher=MATCHER       blocks whose external labels match this matcher
                              will be replicated. All Prometheus matchers are
                              supported, including =, !=, =~ and !~.
      --[no-]single-run       Run replication only one time, then exit.
      --min-time=0000-01-01T00:00:00Z
                              Start of time range limit to replicate. Thanos
                              Replicate will replicate only metrics, which
                              happened later than this value. Option can be a
                              constant time in RFC3339 format or time duration
                              relative to current time, such as -1d or 2h45m.
                              Valid duration units are ms, s, m, h, d, w, y.
      --max-time=9999-12-31T23:59:59Z
                              End of time range limit to replicate. Thanos
                              Replicate will replicate only metrics, which
                              happened earlier than this value. Option can be a
                              constant time in RFC3339 format or time duration
                              relative to current time, such as -1d or 2h45m.
                              Valid duration units are ms, s, m, h, d, w, y.
      --id=ID ...             Block to be replicated to the destination bucket.
                              IDs will be used to match blocks and other
                              matchers will be ignored. When specified, this
                              command will be run only once after successful
                              replication. Repeated field
      --[no-]ignore-marked-for-deletion
                              Do not replicate blocks that have deletion mark.

```

### Bucket downsample

`tools bucket downsample` is used to downsample blocks in an object store bucket as a service. It implements the downsample API on top of historical data in an object storage bucket.

```bash
thanos tools bucket downsample \
    --data-dir        "/local/state/data/dir" \
    --objstore.config-file "bucket.yml"
```

The content of `bucket.yml`:

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

```$ mdox-exec="thanos tools bucket downsample --help"
usage: thanos tools bucket downsample [<flags>]

Continuously downsamples blocks in an object store bucket.


Flags:
  -h, --[no-]help             Show context-sensitive help (also try --help-long
                              and --help-man).
      --[no-]version          Show application version.
      --log.level=info        Log filtering level.
      --log.format=logfmt     Log format to use. Possible options: logfmt,
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
      --http-address="0.0.0.0:10902"
                              Listen host:port for HTTP endpoints.
      --http-grace-period=2m  Time to wait after an interrupt received for HTTP
                              Server.
      --http.config=""        [EXPERIMENTAL] Path to the configuration file
                              that can enable TLS or authentication for all HTTP
                              endpoints.
      --wait-interval=5m      Wait interval between downsample runs.
      --downsample.concurrency=1
                              Number of goroutines to use when downsampling
                              blocks.
      --block-files-concurrency=1
                              Number of goroutines to use when
                              fetching/uploading block files from object
                              storage.
      --data-dir="./data"     Data directory in which to cache blocks and
                              process downsamplings.
      --hash-func=            Specify which hash function to use when
                              calculating the hashes of produced files. If no
                              function has been specified, it does not happen.
                              This permits avoiding downloading some files twice
                              albeit at some performance cost. Possible values
                              are: "", "SHA256".

```

### Bucket mark

`tools bucket mark` can be used to manually mark block for deletion.

NOTE: If the [Compactor](compact.md) is currently running and compacting exactly same block, this operation would be potentially a noop."

```bash
thanos tools bucket mark \
    --id "01C8320GCGEWBZF51Q46TTQEH9" --id "01C8J352831FXGZQMN2NTJ08DY"
    --objstore.config-file "bucket.yml"
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

```$ mdox-exec="thanos tools bucket mark --help"
usage: thanos tools bucket mark --id=ID --marker=MARKER [<flags>]

Mark block for deletion or no-compact in a safe way. NOTE: If the compactor is
currently running compacting same block, this operation would be potentially a
noop.


Flags:
  -h, --[no-]help          Show context-sensitive help (also try --help-long and
                           --help-man).
      --[no-]version       Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt, json or
                           journald.
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
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (mutually
                           exclusive). Content of YAML file that contains
                           object store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --id=ID ...          ID (ULID) of the blocks to be marked for deletion
                           (repeated flag)
      --marker=MARKER      Marker to be put.
      --details=DETAILS    Human readable details to be put into marker.
      --[no-]remove        Remove the marker.

```

### Bucket Rewrite

`tools bucket rewrite` rewrites chosen blocks in the bucket, while deleting or modifying series.

For example we can remove all non counters from the block you have on your disk (e.g in Prometheus dir):

```bash
thanos tools bucket rewrite --no-dry-run \
  --id 01DN3SK96XDAEKRB1AN30AAW6E \
  --objstore.config "
type: FILESYSTEM
config:
  directory: <local dir>
" \
  --rewrite.to-delete-config "
- matchers: \"{__name__!~\\\".*total\\\"}\"
"
```

By default, rewrite also produces `change.log` in the tmp local dir. Look for log message like:

```
ts=2020-11-09T00:40:13.703322181Z caller=level.go:63 level=info msg="changelog will be available" file=/tmp/thanos-rewrite/01EPN74E401ZD2SQXS4SRY6DZX/change.log`
```

```$ mdox-exec="thanos tools bucket rewrite --help"
usage: thanos tools bucket rewrite --id=ID [<flags>]

Rewrite chosen blocks in the bucket, while deleting or modifying series Resulted
block has modified stats in meta.json. Additionally compaction.sources are
altered to not confuse readers of meta.json. Instead thanos.rewrite section
is added with useful info like old sources and deletion requests. NOTE: It's
recommended to turn off compactor while doing this operation. If the compactor
is running and touching exactly same block that is being rewritten, the resulted
rewritten block might only cause overlap (mitigated by marking overlapping block
manually for deletion) and the data you wanted to rewrite could already part of
bigger block.

Use FILESYSTEM type of bucket to rewrite block on disk (suitable for vanilla
Prometheus) After rewrite, it's caller responsibility to delete or mark source
block for deletion to avoid overlaps. WARNING: This procedure is *IRREVERSIBLE*
after certain time (delete delay), so do backup your blocks first.


Flags:
  -h, --[no-]help           Show context-sensitive help (also try --help-long
                            and --help-man).
      --[no-]version        Show application version.
      --log.level=info      Log filtering level.
      --log.format=logfmt   Log format to use. Possible options: logfmt,
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
      --objstore.config-file=<file-path>
                            Path to YAML file that contains object
                            store configuration. See format details:
                            https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                            Alternative to 'objstore.config-file' flag (mutually
                            exclusive). Content of YAML file that contains
                            object store configuration. See format details:
                            https://thanos.io/tip/thanos/storage.md/#configuration
      --id=ID ...           ID (ULID) of the blocks for rewrite (repeated flag).
      --tmp.dir="/tmp/thanos-rewrite"
                            Working directory for temporary files
      --[no-]dry-run        Prints the series changes instead of doing them.
                            Defaults to true, for user to double check. (:
                            Pass --no-dry-run to skip this.
      --[no-]prom-blocks    If specified, we assume the blocks to be uploaded
                            are only used with Prometheus so we don't check
                            external labels in this case.
      --[no-]delete-blocks  Whether to delete the original blocks after
                            rewriting blocks successfully. Available in non
                            dry-run mode only.
      --hash-func=          Specify which hash function to use when calculating
                            the hashes of produced files. If no function has
                            been specified, it does not happen. This permits
                            avoiding downloading some files twice albeit at some
                            performance cost. Possible values are: "", "SHA256".
      --rewrite.to-delete-config-file=<file-path>
                            Path to YAML file that contains
                            []metadata.DeletionRequest that will be applied to
                            blocks
      --rewrite.to-delete-config=<content>
                            Alternative to 'rewrite.to-delete-config-file' flag
                            (mutually exclusive). Content of YAML file that
                            contains []metadata.DeletionRequest that will be
                            applied to blocks
      --rewrite.to-relabel-config-file=<file-path>
                            Path to YAML file that contains relabel configs that
                            will be applied to blocks
      --rewrite.to-relabel-config=<content>
                            Alternative to 'rewrite.to-relabel-config-file'
                            flag (mutually exclusive). Content of YAML file that
                            contains relabel configs that will be applied to
                            blocks
      --[no-]rewrite.add-change-log
                            If specified, all modifications are written to new
                            block directory. Disable if latency is to high.

```

### Bucket Upload Blocks

`tools bucket upload-blocks` uploads a blocks created on the given bucket.

```$ mdox-exec="thanos tools bucket upload-blocks --help"
usage: thanos tools bucket upload-blocks [<flags>]

Upload blocks push blocks from the provided path to the object storage.


Flags:
  -h, --[no-]help              Show context-sensitive help (also try --help-long
                               and --help-man).
      --[no-]version           Show application version.
      --log.level=info         Log filtering level.
      --log.format=logfmt      Log format to use. Possible options: logfmt,
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
      --path="./data"          Path to the directory containing blocks to
                               upload.
      --label=key="value" ...  External labels to add to the uploaded blocks
                               (repeated).
      --[no-]shipper.upload-compacted
                               If true shipper will try to upload compacted
                               blocks as well.
      --shipper.upload-concurrency=5
                               Number of goroutines to use when uploading block
                               files to object storage.

```

## Rules-check

The `tools rules-check` subcommand contains tools for validation of Prometheus rules.

This is allowing to check the rules with the same validation as is used by the Thanos Ruler node.

NOTE: The check is equivalent to the `promtool check rules` with addition of Thanos Ruler extended rules file syntax, which includes `partial_response_strategy` field which `promtool` does not allow.

If the check fails the command fails with exit code `1`, otherwise `0`.

Example:

```
./thanos tools rules-check --rules cmd/thanos/testdata/rules-files/*.yaml
```

```$ mdox-exec="thanos tools rules-check --help"
usage: thanos tools rules-check --rules=RULES

Check if the rule files are valid or not.


Flags:
  -h, --[no-]help          Show context-sensitive help (also try --help-long and
                           --help-man).
      --[no-]version       Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt, json or
                           journald.
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
      --rules=RULES ...    The rule files glob to check (repeated).

```

## Rules-backfill

The `tools rules-backfill` subcommand retroactively evaluates Prometheus recording rules against historical data accessible through a Thanos Query endpoint, produces TSDB blocks from the results, and uploads those blocks to object storage. Alerting rules in the provided rule files are skipped automatically.

This is useful when a new recording rule is deployed and dashboards need historical data immediately, when recording rules must be regenerated after a label migration, or when aggregated metrics need to be reconstructed after data loss. The tool connects to Thanos Query via the HTTP PromQL API, so it benefits from deduplication, fan-out, and partial response handling that Thanos Query already provides.

Unlike `promtool tsdb create-blocks-from rules`, which targets a single Prometheus instance and writes blocks to a local directory, `thanos tools rules-backfill` queries across the full Thanos data layer (Store Gateway, Sidecars, Receivers) and uploads blocks directly to object storage with proper Thanos metadata (external labels, source type, resolution).

Example:

```bash
thanos tools rules-backfill \
    --rules="rules/*.yaml" \
    --query="http://thanos-query:9090" \
    --objstore.config-file="bucket.yml" \
    --start="-180d" \
    --end="-3h" \
    --label='cluster="prod-us-east-1"' \
    --label='backfill_job="2026-04"'
```

### Flags

```
usage: thanos tools rules-backfill [<flags>]

Backfill recording rules against historical Thanos data and upload blocks to
object storage.


Flags:
  -h, --[no-]help          Show context-sensitive help (also try --help-long and
                           --help-man).
      --[no-]version       Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt, json or
                           journald.
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
      --rules=RULES ...    The rule files glob to backfill (repeated). Required.
      --query=QUERY        Thanos Query HTTP endpoint URL. Required.
      --start=START        Start of the backfill time range (RFC3339 or relative
                           duration like -180d). Required.
      --end=-3h            End of the backfill time range (RFC3339 or relative
                           duration like -3h). Default is 3h ago.
      --eval-interval=60s  Evaluation interval for rules. If a rule group
                           specifies its own interval, that value takes
                           precedence.
      --max-block-duration=2h
                           Maximum duration for a single TSDB block.
      --label=key="value" ...
                           External labels to attach to blocks (repeated,
                           format key="value"). Required.
      --dry-run            Create blocks locally but do not upload to object
                           storage.
      --tmp-dir=TMP-DIR    Temporary directory for block creation. Defaults to
                           OS temp dir.
      --dedup              Enable deduplication in queries. Default true.
      --no-dedup           Disable deduplication in queries.
      --partial-response   Enable partial response in queries. Default false.
      --max-source-resolution=0s
                           Maximum source resolution for queries (0s, 5m, 1h).
      --query-timeout=5m   Timeout for individual query requests.
      --retry-attempts=3   Number of retry attempts for failed queries.
      --query-rate-limit=10
                           Maximum queries per second against the Query API.
      --hash-func=         Hash function for block verification (e.g. SHA256).
                           Empty means none.
      --tenant-header=     HTTP header name for multi-tenancy (e.g.
                           THANOS-TENANT). Empty means no tenant header.
      --tenant=            Tenant ID value to send via the tenant header.
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
                           Required unless --dry-run is set.
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (mutually
                           exclusive). Content of YAML file that contains
                           object store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
```

### Usage Examples

**Basic backfill of a recording rule over a fixed time range:**

```bash
thanos tools rules-backfill \
    --rules="rules/aggregations.yaml" \
    --query="http://thanos-query:9090" \
    --objstore.config-file="bucket.yml" \
    --start="2026-01-01T00:00:00Z" \
    --end="2026-04-01T00:00:00Z" \
    --label='cluster="prod-us-east-1"' \
    --label='backfill_job="2026-Q1-agg"'
```

**Using relative time durations:**

```bash
thanos tools rules-backfill \
    --rules="rules/*.yaml" \
    --query="http://thanos-query:9090" \
    --objstore.config-file="bucket.yml" \
    --start="-180d" \
    --end="-3h" \
    --label='cluster="prod"' \
    --label='backfill_job="2026-04"'
```

**Dry-run mode (create blocks locally without uploading):**

```bash
thanos tools rules-backfill \
    --rules="rules/new-rule.yaml" \
    --query="http://thanos-query:9090" \
    --start="-7d" \
    --end="-3h" \
    --label='cluster="prod"' \
    --label='backfill_job="test-run"' \
    --dry-run
```

In dry-run mode, `--objstore.config-file` is not required. Blocks are written to the temporary directory and can be inspected manually.

**Multi-tenant setup with tenant header:**

```bash
thanos tools rules-backfill \
    --rules="rules/tenant-a.yaml" \
    --query="http://thanos-query:9090" \
    --objstore.config-file="bucket.yml" \
    --start="-90d" \
    --end="-3h" \
    --label='tenant="team-a"' \
    --label='backfill_job="2026-04"' \
    --tenant-header="THANOS-TENANT" \
    --tenant="team-a"
```

**Custom evaluation interval and block duration:**

```bash
thanos tools rules-backfill \
    --rules="rules/hourly-agg.yaml" \
    --query="http://thanos-query:9090" \
    --objstore.config-file="bucket.yml" \
    --start="-365d" \
    --end="-3h" \
    --eval-interval="5m" \
    --max-block-duration="2h" \
    --label='cluster="prod"' \
    --label='backfill_job="2026-annual"' \
    --query-rate-limit=5
```

### External Labels and Compaction Groups

External labels on generated blocks determine which compaction group the blocks belong to. The Thanos compactor groups blocks by external labels and resolution, then checks for time-range overlaps within each group. Two blocks in the same compaction group with overlapping time ranges cause the compactor to halt (unless vertical compaction is enabled).

Because backfilled blocks always cover a time range where the bucket already has existing blocks, the external label strategy is critical for safe operation.

**Recommended: Use a unique backfill job label per run.** Add a label such as `backfill_job="<unique-value>"` alongside the labels that identify the data source (e.g., `cluster`, `tenant`). This places backfilled blocks in their own compaction group, separate from live data. No compactor configuration changes are needed.

```bash
# Each run gets its own compaction group -- fully safe
--label='cluster="prod"' --label='backfill_job="2026-04-new-rules"'
```

The extra label is visible on series at query time. A query for `my_rule{cluster="prod"}` still matches backfilled blocks -- the additional label appears in results but does not prevent matching. For the primary use case (backfilling a rule that never existed before), this is the simplest and safest approach.

**Advanced: Same labels as live data.** If you need backfilled series to have the exact same label set as live series (no extra labels), use the same external labels as existing blocks. This requires `--compact.enable-vertical-compaction` to be active on the compactor before the first backfill block is uploaded. Without it, the compactor halts for the entire bucket. The tool warns at startup if it detects live-data blocks with the same compaction group key in the target time range.

All generated blocks carry `thanos.source: "rules.backfill"` in their metadata regardless of the label strategy, providing traceability through `thanos tools bucket inspect`.

### Idempotency and Resumability

The tool is designed to be safely re-run after interruption. On startup, before any block is written, it scans the bucket for existing blocks that match the run's external labels, have source `rules.backfill`, and fall within the target time range. A planned time window is skipped if any existing backfill block's time range overlaps with it.

This works because `block.Upload` writes `meta.json` last. A crash at any point before `meta.json` is written leaves no visible block in the bucket -- both the Store Gateway and compactor ignore ULID directories without `meta.json`. On re-run, the tool detects which windows have blocks and skips them. Orphaned partial directories from interrupted uploads are cleaned up by the compactor's `BlocksCleaner`.

No local checkpoint files are needed. The bucket itself is the checkpoint. The same `--start`, `--end`, `--max-block-duration`, and `--label` flags produce deterministic time window splits, so re-runs with the same parameters skip windows that already have uploaded blocks.

Note: coverage detection is based on block time range overlap, not on whether the block contains all intended rules. If a run is interrupted after uploading a block that contains only a subset of rules for a window (e.g., rule A succeeded but rule B failed before the fix that made query failures abort the window), the next run will skip that window entirely. The current implementation prevents this scenario by failing the entire window on any rule query or append error -- no block is uploaded for a partially successful window.

### Rule Dependencies

If rule B's expression references a metric produced by rule A, and both rules are in the same file being backfilled, rule B will produce empty or incorrect results. Within a single backfill pass, rule A's output is not yet available in Thanos Query -- it must first be uploaded to object storage and synced by the Store Gateway.

To handle dependent rules, run separate invocations:

1. Backfill rule A.
2. Wait for the Store Gateway to sync the new blocks (default sync interval: 3 minutes).
3. Backfill rule B in a separate invocation.

### Per-Group Evaluation Intervals

If a rule group in the provided rule file specifies its own `interval` field, that value is used as the evaluation step for all rules in that group. The `--eval-interval` flag only applies to groups that do not define an explicit interval. This matches Prometheus semantics, where each rule group can have its own evaluation cadence.

```yaml
groups:
  - name: fast-eval
    interval: 15s       # This group uses 15s, ignoring --eval-interval
    rules:
      - record: job:request_rate:rate5m
        expr: sum(rate(http_requests_total[5m])) by (job)

  - name: default-eval   # This group uses --eval-interval (default 60s)
    rules:
      - record: job:error_rate:rate5m
        expr: sum(rate(http_errors_total[5m])) by (job)
```

### Post-Backfill Verification

After the backfill completes, verify that the blocks are correctly uploaded and queryable:

1. **Check block count.** Use `thanos tools bucket inspect` to list blocks produced by the backfill:

   ```bash
   thanos tools bucket inspect --objstore.config-file="bucket.yml" \
       -l backfill_job="2026-04"
   ```

2. **Query the backfilled metric.** Through Thanos Query, run a PromQL query covering the backfilled time range and verify non-empty results. Allow a few minutes for the Store Gateway to sync new blocks (default sync interval: 3 minutes).

3. **Check compactor health.** After the Store Gateway picks up the new blocks, monitor the compactor for halt errors. Check `thanos_compact_group_compactions_failures_total` for at least one hour post-backfill. If you used distinct external labels (recommended), the compactor should process the backfill group without issues.

#### Probes

- The downsample service exposes two endpoints for probing:
  - `/-/healthy` starts as soon as the initial setup is completed.
  - `/-/ready` starts after all the bootstrapping completed (e.g object store bucket connection) and ready to serve traffic.

> NOTE: Metric endpoint starts immediately so, make sure you set up readiness probe on designated HTTP `/-/ready` path.
