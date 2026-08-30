# Stratus CLI

`stratuscli` drives a running [stratus](../../README.md) server over gRPC. It has two faces: a
plain command mode that prints one structured log line per result, and a full-screen TUI console
(`--tui`) with a live stream viewer, live stream info and a command menu.

## Building

```sh
task go-build-cli        # runs sanity, then builds ./dist/cli/stratuscli
go run ./cmd/cli/stratuscli.go --help
```

## In a container

`cli.Dockerfile` builds a CLI-only image; its builder stage runs `task go-build-cli`, so the image
goes through the same sanity gate as a local build.

```sh
task docker-build-cli                       # -> barnowlsnest/stratuscli:latest
docker build -f cli.Dockerfile -t stratuscli .
```

Both modes work in the container. Command mode needs nothing special; the TUI needs a terminal,
so pass `-it`:

```sh
docker run --rm --network stratus-net stratuscli --host stratus info
docker run --rm -it --network stratus-net stratuscli --host stratus --tui
```

`task docker-run-cli` wraps that second form — it builds the image, runs it with `-it` on the host
network, and passes everything after `--` to `stratuscli`:

```sh
task docker-run-cli -- info
task docker-run-cli -- add -k 1 -d "hello world"
task docker-run-cli -- --tui
TAG=dev task docker-run-cli -- info
```

`--host` matters: the default `127.0.0.1` is the container itself. Point it at a service name on a
shared network, at `host.docker.internal` for a server on the host, or run with `--network host`.
The image sets `TERM=xterm-256color` for the TUI; override it with `-e TERM=...`. To read a file
with `addfile`, mount it: `-v "$PWD/records.json:/data/records.json" … addfile -f /data/records.json`.

## Global options

Every option is available as a flag or as an environment variable of the same name, uppercased.
They apply to both modes.

| Flag     | Env    | Default     | Meaning                                                |
|----------|--------|-------------|--------------------------------------------------------|
| `--host` | `HOST` | `127.0.0.1` | stratus hostname                                       |
| `--port` | `PORT` | `8000`      | stratus port                                           |
| `--tui`  | `TUI`  | `false`     | start the full-screen TUI instead of running a command |

The client is built once, before the command runs, over an insecure (no TLS) connection. `SIGINT`
and `SIGTERM` cancel the in-flight call and shut it down.

## Commands

| Command     | Aliases              | Purpose                                           |
|-------------|----------------------|---------------------------------------------------|
| `info`      | `i`                  | print the stream range and record counts          |
| `get`       | `g`, `read`, `range` | read an inclusive range of records                |
| `offset`    | —                    | tail the stream from an id                        |
| `add`       | `a`, `append`        | append one record                                 |
| `addfile`   | `af`                 | append a batch read from a JSON, YAML or CSV file |
| `delete`    | `d`, `del`           | truncate the stream up to an id                   |
| `reconcile` | `re`, `rec`          | rebuild the in-memory cache from the WAL          |

Results are written to stdout as structured JSON log lines; a failed call exits non-zero with the
gRPC error. The examples below elide the `timestamp` field each line carries.

### info

```sh
stratuscli info
{"level":"INFO","message":"stream info","start":1,"end":5,"in_memory":5,"on_disk":5}
```

`in_memory` is the LRU population, `on_disk` the WAL record count.

### get

Reads the inclusive range `[--start-id, --end-id]`; both are required.

| Flag             | Short | Default | Meaning                       |
|------------------|-------|---------|-------------------------------|
| `--start-id`     | `-s`  | —       | first record id (required)    |
| `--end-id`       | `-e`  | —       | last record id (required)     |
| `--read-timeout` |       | `5s`    | server-side bound on the read |

```sh
stratuscli get -s 1 -e 5
{"level":"INFO","message":"record","id":1,"data":"hello"}
```

### offset

Streams records from `--start-id`, blocking for new ones until `--max-records` have arrived or
`--read-timeout` expires. Timeout expiry is a normal end of stream, not an error.

| Flag             | Short | Default | Meaning                                                      |
|------------------|-------|---------|--------------------------------------------------------------|
| `--start-id`     | `-s`  | —       | first record id (required)                                   |
| `--max-records`  | `-m`  | —       | how many records to read before exiting (required, non-zero) |
| `--read-timeout` |       | `5s`    | how long to wait for new records (max 24h)                   |

```sh
stratuscli offset -s 1 -m 100 --read-timeout 30s
```

### add

Appends a single record. A dedup key seen again inside the server's dedup window is dropped, and
a batch that is entirely duplicate fails with `ALREADY_EXISTS`.

| Flag     | Short | Meaning                       |
|----------|-------|-------------------------------|
| `--key`  | `-k`  | non-zero dedup key (required) |
| `--data` | `-d`  | payload (required)            |

```sh
stratuscli add -k 1 -d hello
{"level":"INFO","message":"added record to the stream","id":1}
```

### addfile

Appends a batch in one call. The format is picked from the file extension: `.json`, `.yaml`/`.yml`
or `.csv`; anything else is rejected.

```sh
stratuscli addfile -f records.json
{"level":"INFO","message":"added records to the stream from the file /abs/records.json","duplicates":0,"start":4,"end":5}
```

**JSON / YAML** — a single `input` list of records. `raw_data` may be an inline object or a
string; an inline object is stored as its JSON encoding, so `"raw_data": "{}"` and
`"raw_data": {}` produce the same payload.

```json
{
  "input": [
    {"dedup_key": 2001, "raw_data": {"event": "login", "user": 7}},
    {"dedup_key": 2002, "raw_data": "plain text"}
  ]
}
```

```yaml
input:
  - dedup_key: 2001
    raw_data:
      event: login
      user: 7
  - dedup_key: 2002
    raw_data: plain text
```

**CSV** — headerless, two columns: dedup key and payload. Extra columns are ignored.

```csv
1001,alpha
1002,beta
```

### delete

Truncates the stream up to and including `--end-id` and reports the range actually removed. The
WAL reclaims whole segments, so the reported range can be shorter than requested and ids at the
boundary may stay readable for a while.

```sh
stratuscli delete -e 2
{"level":"INFO","message":"deleted","start":1,"end":1}
```

### reconcile

Reloads the in-memory cache from the WAL over the stream's current range and reports that range.
Useful after a delete or a burst of cold reads.

```sh
stratuscli reconcile
{"level":"INFO","message":"im-memory cache reconciled","start":1,"end":5}
```

## TUI mode

```sh
stratuscli --tui --host 127.0.0.1 --port 8000
```

```
▛▚ S T R A T U S ▞▜──────────────────────────────◢ ONLINE ▏ 127.0.0.1:8000
┌───────────────────────────────────────────────┐┌────────────────────────┐
│▛ STREAM ▏ LIVE ▏ rx 5 ▏ 1.0 rec/s ────────────││▛ INFO ▏ LIVE ──────────│
│23:04:02.719 #00000001 ▸ hello                 ││RANGE   #1 → #5         │
│23:04:02.741 #00000002 ▸ alpha                 ││HELD    5               │
│                                               ││CACHE   5               │
│                                               ││DISK    5               │
│                                               ││SPLIT   ███████╌╌╌  50% │
│                                               ││FLUX    1.0 rec/s       │
│                                               │└────────────────────────┘
│                                               │┌────────────────────────┐
│                                               ││▛ COMMANDS ─────────────│
│                                               ││▶ APPEND             [a]│
│                                               ││  DELETE             [d]│
│                                               ││  RECONCILE          [r]│
└───────────────────────────────────────────────┘└────────────────────────┘
◆ appended #6 (0 duplicate)                          buf 7/5000 · 23:04:07
tab focus · ↑↓ select · enter run · f follow · c clear · q quit
```

**Stream viewer** — seeds with the last 200 records, then tails live: new records appear as any
client appends them. The title tracks follow mode, how many records this session has received and
the current rate. The tail resubscribes on its own and rewinds when a `delete` truncates the
stream under it.

**Info panel** — refreshes every second, and immediately after any command: stream range, records
held, LRU and WAL counts, the cache-vs-disk split, throughput and the last sync time. The header
shows link state; a lost connection is reported in the viewer and the indicator turns red.

**Commands** — `APPEND` and `DELETE` open a small form in the panel; `RECONCILE` runs straight
away. Outcomes and errors land in the status bar, with the gRPC boilerplate stripped. A successful
`DELETE` marks the truncation in the viewer and resumes follow, so a paused viewer snaps back to
what the stream still holds.

| Key                | Action                                                |
|--------------------|-------------------------------------------------------|
| `tab`              | move focus between the viewer and the menu            |
| `↑` `↓` `k` `j`    | select a command (menu focused)                       |
| `enter`            | run the selected command                              |
| `a` `d` `r`        | append / delete / reconcile without touching the menu |
| `f`                | toggle follow; scrolling or the wheel pauses it       |
| `G` `end`          | jump back to the newest record and resume following   |
| `c`                | clear the viewer scrollback                           |
| `q` `esc` `ctrl+c` | quit                                                  |

In a form: `tab` / `shift+tab` move between fields, `enter` submits, `esc` aborts. Numeric fields
are validated before the call is made.
