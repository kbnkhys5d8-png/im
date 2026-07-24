# Search Source V5

Search Source V5 gives the local `wk.plugin.search` process a stable, bounded
source of persisted messages without changing the IM message-apply contract.

## Message scope

- Only messages that were durably stored by WuKongIM are exposed.
- `NoPersist` messages are never exposed to search. If an anomalous stored row
  still carries that flag, the source page fails closed instead of delegating
  filtering to the plugin.
- A source page is fenced by channel authority and by an applied watermark.
  If the physical message tail is ahead of that watermark, the source reports
  `apply_pending` and search stays unavailable instead of serving an ambiguous
  partial result.

## Apply isolation

Channel Raft keeps `WithNotNeedApplied(true)`. The search watermark observer is
an optional side effect after the normal apply index advances; observer errors
never change `ApplyResp`, message persistence, delivery, or ordinary IM startup.

The observer is asynchronous and bounded:

- primary queue: 4096 observations;
- retry/coalescing state: 4096 channels;
- write batch: at most 256 channels;
- transient storage failures use exponential backoff from 100 ms to 5 s and
  rate-limited warnings;
- pending retries make search fail closed, while IM remains available;
- if both bounded states are exhausted, the process remains fail closed. There
  is deliberately no online physical-tail scan. Recovery is allowed only at a
  later safe startup through an already-consumed bootstrap marker.

Shutdown stops new Raft work first, waits for in-flight apply workers, and then
drains the observer. This prevents a successful proposal from being accepted
after the observer has crossed its shutdown boundary.

## Offline bootstrap

Bootstrap runs synchronously after cluster metadata is authoritative and before
the engine, APIs, events, or plugins accept traffic. It has a 30-second startup
budget. Failure or timeout disables search but never prevents IM from starting.

The marker is `search-source-bootstrap-v1.json` in the configured plugin data
directory. It must be a non-empty regular file, mode `0600`, owned by the IM
user, with exactly this shape:

```json
{"version":1,"node_id":1001}
```

Marker states are durable and mutually exclusive:

- pending marker: explicitly authorizes the first bootstrap for this node;
- `.applying`: the marker has been claimed and may be resumed after a crash;
- `.consumed`: bootstrap completed; later startups may reconcile a missed
  observer tail, but only with roster and channel-authority checks before and
  after each update;
- `.window-closed`: no explicit marker was present on first V5 startup. Search
  remains disabled and physical message tails are never silently adopted.
- `.recovery-authorized`: an operator-created, node-bound authorization used
  only while IM is stopped and the original `.window-closed` remains present;
  IM atomically claims it as `.recovery-applying`.
- `.recovery-consumed`: the explicitly authorized closed-window reconciliation
  completed. The original `.window-closed` remains as audit evidence; a failed
  recovery retains `.recovery-applying` and never silently retries a different
  node or data directory.

The first bootstrap advances only an uninitialized zero watermark. A nonzero
watermark behind the physical tail is ambiguous, so bootstrap retains
`.applying`, disables search, and requires operator investigation; it is never
converted into a future consumed-marker recovery tail.

Bootstrap and consumed-marker reconciliation require the same canonical,
non-empty authoritative roster before and after every page. Each node advances
only channels for which it is a replica; it never copies another node's data or
rewrites a nonzero partial watermark. The local committed channel-config table
is protected by an even/odd process-local revision seqlock. Inventory captures
one stable revision and fails closed if any replicated configuration write or
roster change occurs before the page completes. Each node returns only channels
for which it is the current leader. Message requests bind the inventory roster
and revision and recheck both after normal and `not_owner` responses.

The generic Docker install/cutover scripts remain single-node operational
tools. Three-node production rollout uses a separately reviewed one-time
procedure; runtime protocol support does not make those scripts multi-node.

## Local search-plugin security

The reserved `wk.plugin.search` RPC identity is accepted only from the exact
locally launched child process. Linux uses `pidfd` when available and falls back
to repeated `/proc` PID, UID, and start-time validation. Stale socket close
events cannot revoke a replacement connection.

Plugin shutdown keeps RPC alive while sending `/stop`, waits for the exact child
registry, kills only those registered children after the grace deadline, then
stops RPC and closes authorization state. A filesystem watcher cannot launch a
new process after shutdown begins.

## Query kill switch

The host gates only `wk.plugin.search` route `/usersearch`. Set:

```text
SEARCH_LOCAL_FAIL_CLOSED_FILE=/run/tsdd-search-control/usersearch-disabled-v1
```

`/run/tsdd-search-control` must be a real root-owned directory with exact mode
`0700` and should be mounted from the host. With a safe directory and a missing
marker, queries are allowed. Creating the regular, root-owned, non-writable
marker disables queries. Missing configuration, a different path, unsafe
permissions, symlinks, special files, or inspection errors all fail closed with
HTTP 503. Other plugin routes are unaffected.
