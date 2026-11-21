# Open bugs — split for two teammates

Date: 2026-11-21
Status: ready to execute
Base: `fe36288` (clean tree)
Evidence run: `scripts/lab-fleet-run.sh 6 2 6`, workspace `/tmp/lab-fleet-20261121-202649`
(6 clients, 2 managers, 24 sample URLs, `worker-files-n=4`; all clients `rc=0`)

## Goal

Fix the open bugs found in the fleet log with two teammates working in parallel, without
merge collisions and without mixing responsibilities.

## Ground rules

- Java 8 source level. No `var`, records, `List.of`, text blocks.
- `mvn -o test` must pass offline, with no AWS credentials and no AWS calls in tests.
- No new dependencies. No renames of `server.manager`/packages.
- Fix the root cause; do not weaken or delete a failing test to make it pass. If a test encodes
  the old (buggy) behavior, rewrite it to encode the new behavior.
- One bug fix per commit, with the failing test in the same commit set.
- Do not touch `codeExamples/`.

## Bug inventory

| id | severity | symptom | primary files |
|---|---|---|---|
| B1 | HIGH | fleet runs 3 workers where the plan needs 6; ~8 files/worker instead of 4 | `server/WorkerOrchestrator.java`, `server/Manager.java` (seams) |
| B2 | MEDIUM | a full manager repeatedly re-receives and requeues the same `NEW_TASK`, starving a manager with capacity | `server/Manager.java` (`routerTick`, `handleNewTask`) |
| B3 | LOW | finish log says `free slots 0` when one slot is actually free | `server/CompleteClientJob.java` |
| B4 | LOW | a redelivered `DONE_OCR` can finish a client early, leaving another URL's text empty | `server/Manager.java` (`handleDoneOcr`), `server/ClientTask.java`, `server/SplitInputJob.java` |
| B5 | LOW | global `MAX_INSTANCES` cap is advisory across managers (TOCTOU) | `server/WorkerOrchestrator.java` |

## Root causes (already investigated — do not re-litigate)

**B1.** `WorkerOrchestrator.ensure()` computes `needed = ceil(outstanding / workerFilesN)` from
`Manager.totalOutstanding()`, which is **this manager only** (12 images → 3 workers), but compares
it to `port.liveWorkerCount()` / `port.liveInstanceCount()`, which are **fleet-wide** AWS counts.
Each of the two managers logged `3 needed, 3 live fleet-wide` and stopped; the fleet ran 3
workers for 24 images. Introduced by `f51ee92` ("size workers against the whole fleet"), which
globalized the live counts but left the need local. `docs/.../2026-11-21-manager-worker-list-crash-replacement-design.md` §D3
specifies per-manager sizing (`liveWorkers = workerInstanceIds.size()`).

**B2.** `routerTick()` always polls `manager_queue`; a full manager releases with a 5 s delay, but
every manager polls every 1 s, so the full manager can win the visibility race again (log shows
`full: requeueing client f9336e… (3/3)` three times while the other manager had free slots). A
batch received while one slot was free also overflows: 10 received, 1 admitted, 9 requeued.

**B3.** `CompleteClientJob` logs `clients.freeSlots()` **before** the `finally` removes the task.

**B4.** `handleDoneOcr` always decrements `outstanding`, even when `texts.putIfAbsent` proves it
is a redelivery. With URLs `{A,B}`: A result, A redelivered → `outstanding` reaches 0 with B
missing, and the summary writes an empty text for B.

**B5.** `toStart = min(needed - live, MAX_INSTANCES - liveInstances)` is computed independently by
each manager from the same fleet snapshot; simultaneous launches can overshoot. There is no
cross-manager mutual exclusion in the design, so this is **accepted risk**, not a code fix.

## Ownership map (hard boundary)

| path | owner | notes |
|---|---|---|
| `src/main/java/server/WorkerOrchestrator.java` | **A** | A owns the whole file |
| `src/main/java/server/Manager.java` | shared | A owns only the worker seams (`Port.liveWorkerCount`, `Manager.liveWorkerCount`); B owns `routerTick`, `handleNewTask`, `handleDoneOcr` |
| `src/main/java/server/CompleteClientJob.java` | **B** | |
| `src/main/java/server/ClientTask.java` | **B** | add the per-URL awaiting counts |
| `src/main/java/server/SplitInputJob.java` | **B** | populate awaiting counts; do not change the final `ensureWorkers(...)` call |
| `src/test/java/server/WorkerOrchestratorTest.java` | **A** | new file |
| `src/test/java/server/ManagerTest.java` | shared | A: worker-sizing tests + `fleetAwareManager` helper + reconcile tests; B: admission/done-OCR/finish tests |
| `src/test/java/server/ManagerTest.java` helpers | shared | A owns `fleetAwareManager`; B owns `ReleaseFailingQueue` |

Do not edit a file region you do not own. Neither teammate edits the other's hunks. Do not
reformat files.

## Teammate A — worker fleet sizing (B1, B5)

**Deliverable:** two managers with 12 outstanding images each and `worker-files-n=4` launch
3 workers each (6 fleet-wide), each manager stopping only its own workers.

### Changes

1. In `WorkerOrchestrator`, add a pure function:

```java
// ceil(urlCount / workerFilesN) workers, minus this manager's own workers, bounded by the
// fleet headroom. ownWorkers and liveInstances are counts, not booleans.
static int workersToLaunch(int urlCount, int workerFilesN, int ownWorkers, int liveInstances)
```

- returns `0` when `urlCount <= 0`;
- `needed = workersNeeded(urlCount, workerFilesN)`;
- `headroom = MAX_INSTANCES - liveInstances`;
- returns `Math.max(0, Math.min(needed - ownWorkers, headroom))`.

2. `ensure(int outstanding)` uses it with `ownWorkers = workerInstanceIds.size()` and
   `liveInstances = port.liveInstanceCount()`. **Delete the use of `port.liveWorkerCount()`.**

3. Remove `int liveWorkerCount();` from `WorkerOrchestrator.Port`, its implementation in
   `Manager`'s anonymous `Port`, and `Manager.liveWorkerCount()`. Leave `AWS.runningWorkerCount()`
   alone.

4. Update the debug lines to report `ownWorkers`, `needed`, and `liveInstances` (no longer
   "live fleet-wide" for workers).

5. Document B5 in the `workersToLaunch` Javadoc: the headroom check is best-effort; concurrent
   managers can transiently exceed `MAX_INSTANCES`; the lab script's `enforce_cap` remains the
   hard stop; no distributed lock is added.

### Tests

- New `WorkerOrchestratorTest` (fake `Port`, no AWS):
  - `twoManagersEachSizeTheirOwnShare`: shared fake fleet at 2 managers; orchestrator A
    `ensure(12)` launches 3; orchestrator B `ensure(12)` launches 3; fleet = 6.
  - `noLaunchWhenOwnWorkersCoverTheNeed`: `ownWorkers=3`, `ensure(12)` → 0.
  - `headroomClampsTheLaunch`: `liveInstances=8`, `ensure(100)` → 1.
  - `noLaunchAtTheGlobalCap`: `liveInstances=9` → 0.
  - `workersToLaunch` boundary cases: `urlCount=0` → 0; `ownWorkers>needed` → 0.
- In `ManagerTest`: delete the tests that encode fleet-wide sizing and the old seam
  (`ensureWorkersSizesAgainstTheWholeFleet`, `twoManagersShareTheFleetWorkerBudget`,
  `noLaunchWhenTheFleetAlreadyHasEnoughWorkers`, `globalCapHeadroomLimitsTheLaunch`,
  `ensureWorkersDoesNotLaunchWhenTheFleetIsAtTheGlobalCap`,
  `twoManagersRespectGlobalCapHeadroom`) and the `fleetAwareManager` helper. Keep
  `workersNeededFormula` and the reconcile + termination tests; update any override of
  `liveWorkerCount` in those tests, and update expected launch counts in the reconcile tests to
  the per-manager sizing (`reconcileManager` now needs only `liveInstanceCount`).

### Acceptance

- Unit tests above pass.
- On the lab run, each manager's log shows three launches for its own images; `nodes:`
  reports 6 workers; `worker-files-n=4` is respected (24 images / 6 workers).
- B5 behavior is documented, not "fixed" with a lock.

## Teammate B — admission gate, result accounting, diagnostics (B2, B3, B4)

**Deliverable:** a full manager never takes a request it cannot admit; a redelivered result
never completes a client early.

### B2 — poll gate

In `Manager.routerTick()`:

```java
int slots = clients.freeSlots();
// done-OCR is always polled. When there is no slot, long-poll it so the loop does not spin.
List<Received> results = doneOcrQueue.receive(RECEIVE_MAX, slots > 0 ? 0 : RECEIVE_WAIT_SECONDS);
// ... handle results, return if !running ...
if (slots <= 0) {
    return;                                        // do not receive a request we cannot admit
}
List<Received> requests = managerQueue.receive(Math.min(RECEIVE_MAX, slots), RECEIVE_WAIT_SECONDS);
```

`freeSlots()` only grows on the IoWorker thread (a finish), never during `routerTick`, so the
start-of-tick value is a safe lower bound; no recompute is needed. Do not add a busy-wait: the
long-poll of done-OCR is what blocks the loop while full.

- Keep `handleNewTask`'s `!hasCapacity()` requeue branch and `REQUEUE_DELAY_SECONDS` as defense;
  it is unreachable from a normal tick after this gate.
- Rewrite `ManagerTest` (do not leave a test that asserts the old per-tick requeue):
  - `admitsUpToMaxClientsAndLeavesTheRestQueued`: with 2 free slots and 3 queued, the tick
    receives 2; the third stays on `manager_queue`; `getRejectedClients()==0`.
  - `fullManagerDoesNotReceiveNewTask`: full manager does not receive the new request at all;
    no client reply; `getRejectedClients()==0`; the request is still on the queue.
  - Delete `failedRequeueLeavesTheRequestUndeletedAndUnanswered` and the `ReleaseFailingQueue`
    helper; the release branch remains as a defensive guard, verified by inspection.
  - Add `fullManagerStillDrainsDoneOcr`: fill the slot, send a `DONE_OCR`, tick; the client is
    answered even though `manager_queue` is not polled.

Accepted limit (state it in the test comment): while full, the manager does not see `TERMINATE`
until a slot frees. This matches the capacity design's accepted limit.

### B3 — finish log

In `CompleteClientJob`, drop the `free slots` field from the success debug line (or log after
`clients.remove`). Do not change slot-release ordering: removal stays in `finally`, always runs.

### B4 — per-URL awaiting counts

1. `ClientTask`: add a final `ConcurrentHashMap<String, AtomicInteger> awaiting`. It is
   populated by `SplitInputJob` **before** any `NEW_IMAGE_TASK` is sent; no other writer.
2. `SplitInputJob`, after `task.urls = urls;` and before the send loop: for each URL,
   `awaiting.computeIfAbsent(url, ...).incrementAndGet()`.
3. `handleDoneOcr`:

```java
AtomicInteger remaining = task.awaiting.get(msg.getImageURL());
if (remaining == null || remaining.getAndDecrement() <= 0) {
    logDebug("ignoring duplicate DONE_OCR for client " + ... + " url=" + ...);
    return;   // do NOT decrement outstanding
}
task.texts.putIfAbsent(url, text);
int left = task.outstanding.decrementAndGet();
```

Keep the existing `ERROR:` prefixing and `putIfAbsent` first-wins behavior.

4. Tests in `ManagerTest`:
   - `duplicateResultForOneUrlDoesNotCompleteBeforeAnotherUrl`: input `{A,B}`; deliver A twice,
     tick, assert no `DONE_TASK`; deliver B, tick, assert one `DONE_TASK` and the summary
     contains both A and B text.
   - Keep `duplicateUrlsAreTwoTasks` passing: two input occurrences of A need two results.
   - Keep `duplicateDoneOcrYieldsExactlyOneSummary` passing.

## Merge order and conflicts

1. A branches from `fe36288`, implements, runs `mvn -o test`, commits per bug.
2. B branches from `fe36288`, implements, runs `mvn -o test`, commits per bug.
3. Merge A first. B rebases on the merged A branch; resolve only `ManagerTest.java` import/hunk
   conflicts. If a conflict is in a region B does not own, keep A's version.
4. Final gate: `mvn -o test` on the merged tree.

## End-to-end verification (after both merge)

Run, from a clean workspace (note: deleted SQS queue names need ~60 s before reuse):

```
scripts/lab-fleet-run.sh 6 2 6
```

Expected in the log:

- `nodes: 2 manager(s), 6 worker(s)` (brief 7 while the last worker boots is tolerated).
- each manager's launch lines add up to 3 of its own workers.
- no repeating `full: requeueing client …` from one client (B2).
- 6 clients `rc=0`, 4 URLs each, 24 `DONE_OCR` total.
- default sample-input 404/DNS exceptions are unchanged and expected.

Record the new wall-clock time and worker count in the run notes; compare with ~4.5 min / 3
workers from `/tmp/lab-fleet-20261121-202649`.

## Out of scope (do not fix here)

- Redundant `Manager.jar` / `Worker.jar` upload by every client that starts a manager.
- Client-side deadline for a task that is never finished (known design gap).
- A restarted manager losing its in-memory worker list.
- EC2 tag eventual consistency for `free` slots.
- Strict global `MAX_INSTANCES` enforcement across managers (B5 accepted risk).
- More than two managers; `worker-files-n` enforcement per worker beyond the fleet arithmetic.
