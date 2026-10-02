# Shared storage and embedded buckyd

Shared storage is opt-in and experimental. The classic file backend remains the
default. One go-carbon process owns a Pebble database containing the metric
catalog and compressed archive slots. Persistence, carbonserver and the embedded
buckytools transfer API use that same handle. Do not open the database from a
standalone buckyd or another process.

```toml
[whisper]
storage-backend = "pebble"
store-dir = "/var/lib/graphite/shared"
store-cache-size = 67108864
store-memtable-size = 67108864
# Existing schemas-file, aggregation-file and worker settings still apply.

[buckyd]
enabled = true
bind = "127.0.0.1:4242"
auth-jwt-secret-file = "/etc/go-carbon/buckyd.secret"
node = "carbon-01"
nodes = ["carbon-01:2003=a", "carbon-02:2003=a"]
hash = "carbon"
replicas = 1
tmpdir = "/var/lib/graphite/transfer-tmp"
max_transfers = 4
max_body_bytes = 167772160
```

The process must be able to write the store and temporary directories. An empty
`store-dir` selects `whisper.data-dir + "/.store"`. Backend, store memory/path
and buckyd settings require restart; ordinary schema changes apply to new
metrics. Existing policies are preserved. Online policy migration is rejected.

Writes may arrive out of order; archive updates use the shared WAL and require
no per-metric sidecars. The persister confirms a cache batch only after a synced
store commit and requeues failed writes. This durability boundary starts at
persistence, not at receiver acknowledgement of an in-memory cache write.

Carbonserver builds trie/trigram indexes from the catalog and uses shared-store
reads for render/info. The normal periodic scan discovers newly persisted
metrics; realtime/cache discovery options retain their existing behavior.
Transfer mutations schedule a catalog refresh at most once every 30 seconds;
the normal `scan-frequency` remains active. Imports complete after the store
commit, and the transfer temporary file is then removed. New metrics may take
up to the next batched scan to appear in find/glob results. Shared mode
bypasses response caches so deletion and replacement cannot leave cached
results from an earlier generation. This affects performance and should be
measured on representative workloads before rollout.

Metric count, data-point and logical-size quotas use classic Whisper capacity,
including headers. Namespace physical-size quotas are rejected: compressed
tables and the WAL are shared across metrics. `storage.diskBytes`,
`storage.walBytes` and `storage.memTableBytes` report store-wide accounting.
Per-metric physical size and file modification time have no shared equivalent;
the transfer metadata reports classic export size, mode `0644`, and mtime `0`.

## Mapping standalone buckyd options

| Standalone option | go-carbon `[buckyd]` setting |
| --- | --- |
| `--bind`, `-b` | `bind` |
| `--tmpdir`, `-t` | `tmpdir` |
| `--node`, `-n` | `node` (empty selects hostname) |
| positional ring members | `nodes` array, same `HOST[:PORT][=INSTANCE]` syntax |
| `--hash`, `--replicas` | `hash`, `replicas` |
| `--auth-jwt-secret-file` | `auth-jwt-secret-file` |
| `--pprof` | `pprof` (empty disables the dedicated listener) |
| `--pyroscope` | `pyroscope` |
| `--timeout` | `timeout`; accepted legacy cache TTL, unnecessary for catalog reads |
| `--prefix`, `-p`, `--cache_path` | `prefix`, `cache_path`; nonempty values are rejected |
| `--sparse`, `--compressed`, `--mtime` | `sparse`, `compressed`, `mtime`; true is rejected |

Filesystem options do not select shared compression or a per-metric directory.
Shared compression is provided by Pebble; network transfers independently
negotiate Snappy. The embedded API provides `/metrics`, `/metrics/{name}` and
`/hashring`, including listing/filtering, HEAD/GET, POST fill, PUT replacement,
DELETE and offloaded copying. Fill preserves existing destination values and
rejects different retention/aggregation/XFF policies with HTTP 409. Replacement
publishes a complete archive generation atomically.

JWT tokens use the existing `X-Buckyd-Authorization` header, HMAC shared secret,
namespace patterns and `read`, `update`, `replace`, `delete` operation grants.
An empty secret-file setting disables authentication, matching standalone
buckyd. Offloaded transfers mint a short-lived token granting `read` for only
the requested source metric; source and destination must share a secret.
Offload sources are independent of `nodes`, which describes the ingestion
hashring using its original ports and instances. Cross-ring `bucky copy
-offload` works with `nodes` omitted; source redirects are rejected. Missing
source metrics return HTTP 404 so the client's `-ignore404` option applies.
pprof runs on its separate configured listener.

## Migration and client usage

Use the updated **bucky client** with the existing standalone buckyd on file
nodes and embedded buckyd on shared nodes. The standalone daemon is unchanged.
Metric bodies remain classic Whisper exports, preserving all stored archive
points rather than resampling through render. Compressed file sources remain
supported through go-whisper's snapshot importer; compressed `Mix` policies are
rejected.

```sh
bucky copy -src old-node:4242 -dst shared-node:4242 -workers 4 \
  -api-token-file /etc/graphite/bucky.token
# After routing/cutover is ready, normal rebalance/copy -delete can retire sources.
```

GET supplies a revision token; offloaded client moves with deletion obtain one via HEAD.
The updated client sends that token with source DELETE. A write, replacement or
delete/recreate during transfer causes HTTP 409 and retains the source. Failed
copy/delete jobs remain retryable in the client job log. File daemons without
tokens keep their existing deletion behavior. Quiesce ingestion or change
routing before moving active metrics: revision checks detect concurrent writes
but do not implement dual-write routing or merge the receiver cache.

The API uses bounded concurrent temporary exports/imports. Size `tmpdir` for
the classic export capacity of the largest metrics and transfer concurrency;
Snappy exports may also need a second temporary file. `max_body_bytes` bounds
both encoded and decoded imports; increase it deliberately for larger archives.

The root go-whisper and nested `store` modules are pinned to the same commit in
`go.mod` and vendored together. Buckytools is a separate client executable;
go-carbon does not import or require that module.
