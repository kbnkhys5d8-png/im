# Low-Risk Message Sync Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce allocations and conversation-sync CPU usage, and remove one cluster error data race without changing message ranges, ordering, interfaces, or persistence behavior.

**Architecture:** Keep the current message sequence reads unchanged. Add bounded slice preallocation, replace repeated conversation scans with first-wins typed lookup maps while preserving source order, and pass remote errors through a buffered channel so goroutines never share-write an error variable.

**Tech Stack:** Go 1.26.5, PebbleDB, standard `testing`, Testify, race detector, Go benchmarks.

## Global Constraints

- Do not change HTTP paths, request/response fields, status codes, or JSON shapes.
- Do not change message/conversation ordering, unread calculations, result limits, nil/empty behavior, or persistence.
- Do not remove either existing `GetChannelLastMessageSeq` call in this batch.
- Do not change metric increments.
- Do not parallelize local and remote queries or change forwarding contexts.
- Do not add schema, configuration, or dependencies.
- Preserve `internal/api/task_migrate.go` byte-for-byte; baseline SHA-256 is `fb53972ad9347aed0ae2365295785e7dc0114cb2b7fe037c25f932ab6a52c7f6`.

## Recorded Baseline

- `go test ./internal/api -count=1`: PASS.
- `go test ./pkg/wkdb -count=1`: existing failures:
  - `TestAddPluginUsers`
  - `TestCacheStats`
  - `TestDeviceCacheStats`
  - `TestGetPluginUsers`
  - `TestPermissionCacheStats`
  - `TestSearchDevices`
  - `TestTruncateLogTo`
  - `TestUpdateConversationsInCacheWithNewConversation`
- Acceptance requires no additional failing test names.

## File Map

- Modify: `pkg/wkdb/message.go`
- Create: `pkg/wkdb/message_internal_test.go`
- Create: `pkg/wkdb/message_bench_test.go`
- Modify: `internal/api/conversation.go`
- Create: `internal/api/conversation_lookup_test.go`
- Modify: `internal/api/request.go`
- Create: `internal/api/request_parallel_test.go`
- Modify: `docs/superpowers/plans/2026-07-27-low-risk-message-sync-optimization.md`

---

### Task 1: Bounded Message Result Preallocation

**Files:**
- Modify: `pkg/wkdb/message.go`
- Create: `pkg/wkdb/message_internal_test.go`
- Create: `pkg/wkdb/message_bench_test.go`

**Produces:**

```go
const maxMessageResultPrealloc = 128

func messageResultCapacity(limit int, minSeq, maxSeq uint64) int
```

- [x] Add a table-driven `TestMessageResultCapacity` covering disabled limits, empty/reversed ranges, narrow ranges, common pages, and the cap.
- [x] Run the test and confirm RED because the helper is undefined or lacks range arguments.
- [x] Add the minimal bounded-capacity helper.
- [x] Replace only the two `make([]Message, 0)` result allocations in `LoadPrevRangeMsgs` and `LoadNextRangeMsgs`.
- [x] Run the helper test and existing range tests.
- [x] Add `BenchmarkLoadPrevRangeMsgs100` and `BenchmarkLoadNextRangeMsgs100`; setup occurs outside timing and results are retained.
- [x] Record at least six samples with `-benchmem -count=6`.

Acceptance:

- No message range, count, or order changes.
- `limit=0` behavior remains exactly as the current implementation.
- `allocs/op` and `B/op` do not regress.
- At least one allocation metric improves; otherwise revert the production change.

---

### Task 2: First-Wins Conversation Lookup

**Files:**
- Modify: `internal/api/conversation.go`
- Create: `internal/api/conversation_lookup_test.go`

**Produces:**

```go
type conversationChannelKey struct {
	channelID   string
	channelType uint8
}

func indexFirstRecentMessages(
	messages []*channelRecentMessage,
) map[conversationChannelKey]*channelRecentMessage
```

- [x] Add RED tests proving duplicate channels keep the first result and equal IDs with different channel types remain separate.
- [x] Add a reference nested lookup in test code and compare it with the new lookup for `0`, `1`, `100`, `500`, and `1000` entries.
- [x] Add assembly-level lookup tests for normal group channels and fake person channels.
- [x] Implement the typed key and first-wins index.
- [x] Replace recent-message nested scans in both conversation sync paths while continuing to iterate the original conversation slice.
- [x] Replace the cached-conversation existence scan with a typed set, adding every appended key immediately.
- [x] Do not change `channelLastMsgMap` key construction or unread calculations.
- [x] Benchmark the complete operation: map construction plus all lookups versus the complete nested scan.

Acceptance:

- Old reference and new implementation select identical objects in identical output order.
- Duplicate channels keep the first result.
- Normal group and fake person-channel responses remain compatible.
- At 500 and 1000 entries, the indexed complete operation is faster without significant allocation regression.

---

### Task 3: Race-Free Remote Error Collection

**Files:**
- Modify: `internal/api/request.go`
- Create: `internal/api/request_parallel_test.go`

- [x] Replace shared `reqErr` writes with a buffered `errCh` sized to the peer-group count.
- [x] Each remote goroutine sends at most one non-nil error.
- [x] Wait for all goroutines, close the channel, then return the first received error.
- [x] Log the received request error, not the unrelated outer `err`.
- [x] Keep remote requests, result locking, wait behavior, local-query order, and any-error-fails behavior unchanged.
- [x] Run `go test ./internal/api -count=1`.
- [x] Run `go test -race ./internal/api -count=1`.

Acceptance:

- No shared concurrent error write remains.
- `internal/api` stays green under normal and race runs.
- No request scheduling or response-order change.

---

### Task 4: Final Verification

- [x] Run `gofmt` only on changed Go files.
- [x] Run focused new tests.
- [x] Run `go test ./internal/api -count=1`.
- [x] Re-run `go test -json ./pkg/wkdb -count=1` and compare failing test names with the recorded baseline.
- [x] Run `go test -race ./internal/api -count=1`.
- [x] Run `go vet ./pkg/wkdb ./internal/api`.
- [x] Run `git diff --check`.
- [x] Verify `shasum -a 256 internal/api/task_migrate.go` still equals the recorded hash.
- [x] Inspect `git status --short` and ensure only planned files plus the pre-existing migration file are present.

## Rollback

The three production changes are independent. Revert the relevant commit or restore only its scoped files; no database, configuration, or data rollback is required.

## Deferred

- Removing duplicate channel-last-sequence reads.
- Local/remote query parallelism.
- Global forwarding-context changes.
- Conversation database/index/cache redesign.
- API limit behavior changes.
