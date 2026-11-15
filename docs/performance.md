# Stratus performance and limits

Measured on one machine with stratus `cfb3634` (go-wallib v1.5.2) and the default configuration:
Apple M1 Max (10 cores), 64 GB RAM, macOS, local SSD. Records are 1 KiB unless noted, and each run
lasts 15–20 s. The load generator ran on the same machine and used up to about 2 cores, so treat
the numbers as a guide. A Linux server with a different disk will differ, especially for writes,
which are bound by fsync.

CPU is in cores (100% = one core). RSS is the server's peak resident memory.

## Throughput

| Load                                                       | CPU  | RSS    | Throughput                                       |
|------------------------------------------------------------|------|--------|--------------------------------------------------|
| Idle, empty WAL                                            | 0%   | 15 MB  | —                                                |
| Writes, 1 client, 1 record per `Add`                       | 10%  | 160 MB | 126 records/s                                    |
| Writes, 16 clients, 1 record per `Add`                     | 49%  | 165 MB | 2,177 records/s                                  |
| Writes, 64 clients, 1 record per `Add`                     | 58%  | 167 MB | 2,923 records/s                                  |
| Writes, 16 clients, 100 records per `Add`                  | 107% | 606 MB | 80,200 records/s                                 |
| Writes, 64 clients, 100 records per `Add`                  | 187% | 856 MB | 121,300 records/s                                |
| Writes, 16 clients, 10 × 64 KiB records per `Add`          | 106% | 792 MB | 5,864 records/s (375 MB/s)                       |
| Reads in the cache, 16 clients, 64 records per `ReadRange` | 376% | 241 MB | 1.27M records/s (19.8k requests/s)               |
| Reads in the cache, 64 clients                             | 389% | 286 MB | 1.60M records/s (24.9k requests/s)               |
| Reads outside the cache, 16 clients, 1.5 GB WAL            | 943% | 716 MB | 132 records/s                                    |
| 256 tails + 16 writers                                     | 129% | 186 MB | 2,109 writes/s, 540k records/s out               |
| 512 tails + 16 writers                                     | 185% | 239 MB | 1,788 writes/s, 916k records/s out               |
| 1,024 tails + 16 writers                                   | 237% | 272 MB | 1,209 writes/s, 1.24M records/s out              |
| Mixed: 32 writers, 16 readers, 256 tails                   | 325% | 891 MB | 9.7k writes/s, 219k reads/s, 2.49M records/s out |

The tails are `ReadOffset` streams spread over 8 client connections. Every tail kept up with the
write rate.

## What one core handles

These are estimates from the measured cost per operation. One core (100% CPU) handles about:

- **3,000 single-record `Add` calls/s**, or **75,000 records/s** when each `Add` carries about 100
  records.
- **330,000 records/s read from the cache**, which is about 5,000 `ReadRange` calls of 64 records.
- **Tail fan-out costs about 1.5 µs per record delivered to each tail.** For example, 2,000 writes/s
  with 150 tails fits in one core.
- **Reads outside the cache are not covered:** 16 such readers used 9 cores for 132 records/s.

## Sizing one server

| Workload                                                     | Cores | RAM    |
|--------------------------------------------------------------|-------|--------|
| Up to 3k writes/s, up to 150 tails, reads within the cache   | 1     | 512 MB |
| Up to 50k records/s in batches, up to 500 tails, cache reads | 2–4   | 2 GB   |
| 100k+ records/s in batches, about 1,000 tails, mixed load    | 4+    | 4 GB   |

RAM is mostly made of three parts:

- **Cache:** `cache_size` × record size, which is 4 MB at the defaults with 1 KiB records and
  256 MB with 64 KiB records.
- **Dedup keys:** about 100 B per key, and every record's key is kept for `dedup_window` (default
  1 min). 100k records/s keeps about 6M keys, or roughly 600 MB.
- **Follower queue:** up to `subscriber_buffer` (default 1024) waiting records, each with a copy of
  its payload.

Go's garbage collector can roughly double the live heap. Set `GOMEMLIMIT` to about 80% of the
memory you give the server.

## Limits

- **Single node.** One stream, one WAL and one server, with no replication, failover or sharding.
- **Record and response size.** gRPC's default 4 MiB receive limit caps a single `Add`, and also any
  `ReadRange` response that a default client receives. For example, 64 records of 64 KiB fail with
  `RESOURCE_EXHAUSTED`.
- **Writes are fsynced before `Add` returns.** Group commits hold up to `wal_batch_size` requests.
  One client doing single-record `Add` calls gets about 126 writes/s here, so batch records.
- **`Add` returns once its records are in the cache**, so they are readable as soon as it returns.
- **Reads outside the cache are slow.** Each record outside the LRU is read by scanning its WAL
  segment (up to `wal_max_segment_size`, default 64 MB) from the start. Size `cache_size` to cover
  how far behind readers and tails can fall.
- **Startup reads the whole WAL** to warm the cache: about 5 s for 7.2 GB here, with the files
  already in the OS page cache.
- **`ReadOffset`** sends at most `max_records` − 1 records per request, and its timeout must be
  between 61 ms and 24 h.
- **`Delete` cannot empty the stream:** the record at `end_id` stays.
- **`max_batch_read_size` is not wired up** and has no effect.

## Scaling

Client side, which gives the biggest gains:

1. **Batch records in each `Add`.** At 100 records per call, throughput rises about 30× per client.
2. **Share connections.** Run many `ReadOffset` streams over a few connections per client process
   (8 worked well). One connection per stream roughly doubles the server's CPU per record.
3. **Keep reads inside the cache.** Raise `cache_size` instead of reading old ranges.

Server side:

- **More cores help reads and tail fan-out.** Writes all go through one WAL writer and fsync, so
  extra cores don't raise single-record write throughput; batching does.
- **To go beyond one server**, run one stratus server per stream or partition and route keys to them
  on the client side. There are no built-in replicas.

Known limits in the code that would raise these ceilings:

- an index inside each WAL segment, for reads outside the cache;
- a cheaper dedup structure, since today each key gets its own timer;
- less locking in the LRU, which takes one exclusive lock per `Get`.
