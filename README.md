# Barn Owls Nest / Stratus

Stratus is an append-only record stream served over gRPC and persisted in a write-ahead log.
Records are appended with a client-supplied dedup key, assigned a monotonic ID (the WAL LSN),
and read back by ID — either as a bounded range or as a live tail that blocks until new records
arrive. A LRU cache in front of the WAL serves recent reads without touching the filesystem.

## Concepts

- **Record ID** — the LSN the WAL assigns on append. IDs are monotonic and start at 1; `0` is
  never a valid record ID and is used as a "not set" sentinel in requests.
- **Dedup key** — a non-zero, client-generated `uint64`. A key seen again within the dedup window
  (`dedup_window`, default `1m`) causes the record to be dropped and counted as a duplicate.
- **Stream range** — the inclusive `[start, end]` window of IDs currently in the WAL. `Delete`
  moves `start` forward; `Add` moves `end` forward.

## API

The service is `stratus.v1.StreamService`, defined in [`proto/stratus/v1/stratus.proto`](proto/stratus/v1/stratus.proto).

| RPC              | Kind          | Purpose                                                         |
|------------------|---------------|-----------------------------------------------------------------|
| `Add`            | unary         | Append a batch of records.                                      |
| `ReadRange`      | unary         | Read the inclusive ID range `[start_id, end_id]`.               |
| `ReadOffset`     | server stream | Read up to `max_records` from `start_id`, tailing for new ones. |
| `Delete`         | unary         | Drop records up to and including `end_id`.                      |
| `ReconcileCache` | unary         | Rebuild the in-memory cache from the WAL.                       |
| `GetStreamInfo`  | unary         | Report the current range and record counts.                     |

### Add

Takes `repeated InputRecord records`, each with a non-zero `dedup_key` and non-empty `raw_data`.
Returns `added_records` (the IDs assigned to records that were written), `stream_records` (the
stream's range after the write), and `duplicate_records` (how many were dropped by the dedup
window).

An empty batch is `INVALID_ARGUMENT`. A batch where *every* record is a duplicate is
`ALREADY_EXISTS` — no partial result is returned. A batch that is partially duplicate succeeds,
and `added_records` covers only the records that made it in.

### ReadRange

Reads `[start_id, end_id]` inclusive. Either bound may be `0`, meaning "the stream's current
first / last ID", so `{}` reads the whole stream.

The range is clamped to the stream's window: a range that overlaps the window is trimmed to it,
while a range entirely outside it is `OUT_OF_RANGE`. Records are served from the LRU cache,
falling back to a single-record WAL read (and caching the result) on a miss, so there is no
fixed per-request record cap.

`timeout` bounds the read on the server side; when unset or non-positive, the read is bounded
only by the caller's context.

### ReadOffset

Streams `ReadResponse` messages starting at `start_id` until `max_records` have been sent or the
timeout expires. Unlike `ReadRange`, it does not end at the current tail: when it catches up it
blocks until `Add` produces more records, then keeps sending. Records already deleted from under
the reader (`start_id` below the stream's first ID) end the stream with `OUT_OF_RANGE`.

`max_records` must be non-zero (`INVALID_ARGUMENT` otherwise). `timeout` is clamped to a floor of
61ms and must not exceed 24h. Timeout expiry is a normal end of stream, not an error.

### Delete

Removes records up to and including `end_id` and evicts them from the cache. Returns
`deleted_records` and the remaining `stream_records`. An `end_id` of `0`, or one outside the
current stream range, is `OUT_OF_RANGE`.

Deletion is backed by the WAL's truncate plus a cut at the offset, so it reclaims whole segments:
`deleted_records` reports what was actually removed, which can be a shorter range than requested,
and IDs at the boundary may briefly remain readable.

### ReconcileCache

Clears the cache and reloads it from the WAL over the stream's current range, then returns that
range. Useful after a `Delete` or when the cache has been churned by cold reads. The reload
streams the whole range record by record, so it is not bounded by the read cap; the LRU keeps the
newest `cache_size` entries. The startup preload works the same way.

### GetStreamInfo

Returns the stream `range`, `cached_records` (entries currently in the LRU) and `fs_records`
(records in the WAL).

### Status codes

| Code                             | Cause                                                                        |
|----------------------------------|------------------------------------------------------------------------------|
| `INVALID_ARGUMENT`               | empty batch, empty record or dedup key, `max_records` of 0, timeout over 24h |
| `OUT_OF_RANGE`                   | requested range lies outside the stream window                               |
| `ALREADY_EXISTS`                 | every record in the batch was a duplicate                                    |
| `DEADLINE_EXCEEDED` / `CANCELED` | context expired or cancelled                                                 |
| `INTERNAL`                       | anything else (WAL failures)                                                 |

## Go client

`pkg/stratusv1` wraps the generated stubs with its own DTOs, so callers do not depend on the
protobuf package.

```go
c, err := stratusv1.Dial("127.0.0.1:8000", stratusv1.WithInsecure())
if err != nil {
    return err
}
defer c.Close()

added, err := c.Add(ctx, []stratusv1.InputRecord{
    {DedupKey: 1, RawData: []byte("hello")},
})

// Bounded read.
records, err := c.ReadRange(ctx, added.AddedRecords.Start, added.AddedRecords.End, 0)

// Live tail: ReadOffset returns a channel closed when the stream ends.
ch, err := c.ReadOffset(ctx, 1, 100, 30*time.Second)
for r := range ch {
    fmt.Println(r.ID, string(r.RawData))
}
```

`Dial` gives the client ownership of the connection (`Close` shuts it down); `New` wraps a
connection you keep owning (`Close` is then a no-op).

## Configuration

Settings come from flags or environment variables of the same name (uppercased).

| Name                   | Default     | Meaning                                                                                                                    |
|------------------------|-------------|----------------------------------------------------------------------------------------------------------------------------|
| `host`                 | `127.0.0.1` | listen host                                                                                                                |
| `port`                 | `8000`      | listen port                                                                                                                |
| `wal_dir`              | —           | WAL segment directory (required)                                                                                           |
| `log_level`            | `info`      | log level                                                                                                                  |
| `dedup_window`         | `1m`        | how long a dedup key is remembered                                                                                         |
| `cache_size`           | `4096`      | LRU capacity in records                                                                                                    |
| `wal_batch_size`       | `16`        | WAL records per fsync batch                                                                                                |
| `wal_max_segment_size` | `64MB`      | segment size before rotation                                                                                               |
| `wal_max_record_size`  | `8MB`       | largest single record the WAL accepts                                                                                      |
| `max_batch_read_size`  | `1024`      | **not wired up**: the storage read cap stays at its default of 64; it bounds single `Read` batches only, not cache warming |

## Running

```sh
task go-build-app             # runs sanity (fmt, vet, lint, test), then builds ./dist/app/stratus
task go-build-cli             # same, for ./dist/cli/stratuscli
task sanity                   # fmt, vet, lint, test
task buf-gen                  # regenerate gRPC code from proto
task docker-run               # build the image and start it via compose
task docker-build-cli         # build the CLI-only image from cli.Dockerfile
task docker-run-cli -- info   # run the CLI in a container; args after `--` go to stratuscli
task clear                    # remove ./dist
```

The image built by `app.Dockerfile` carries the server only; its builder stage installs `task` and
`golangci-lint` (versions pinned as build args) and runs `task go-build-app`, so the image build
goes through the same sanity gate as a local build. Compose is for local runs only: it starts the image `task docker-build` produces. The service
publishes `8000` and runs with `WAL_DIR=/usr/wal`; note that the `stratus_wal` volume is mounted at `/app/config`, so the WAL
directory itself is not on the volume. `cli.Dockerfile` builds a separate CLI-only image that
runs in both modes — see [`cmd/cli/README.md`](cmd/cli/README.md).

## CI

[`.github/workflows/docker.yml`](.github/workflows/docker.yml) lints the tree with `golangci-lint`
first; only then does it build both images, on every pull request, publishing them from `main` and
`v*` tags to GHCR as `ghcr.io/barnowlsnest/stratus` and `ghcr.io/barnowlsnest/stratuscli`. Each
image is tagged with the commit it was built from (`:<sha>`), so a `v*` tag publishes under the
commit that tag points at. It needs no secrets — the job's `packages: write` scope on
`GITHUB_TOKEN` is enough. Since each image build runs `task sanity` in its builder stage, the
workflow doubles as the test gate.

Packages are created private on first publish; make them public under the package's settings on
GitHub, or `docker login ghcr.io` with a PAT that has `read:packages` before pulling.

## CLI

`stratuscli` (in [`cmd/cli`](cmd/cli)) speaks to a running server: one command per RPC, plus a
full-screen TUI console (`--tui`) with a live stream viewer, live stream info and an
append/delete/reconcile menu. See [`cmd/cli/README.md`](cmd/cli/README.md).

```sh
stratuscli info
stratuscli add -k 1 -d hello
stratuscli offset -s 1 -m 100 --read-timeout 30s
stratuscli --tui
```
