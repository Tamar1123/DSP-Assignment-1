# Open items and TODOs

Date: 2026-11-18, re-audited 2026-11-21
Baseline: HEAD `89702bf` plus the working tree.
Rendered: `notes/2026-11-18-open-items-and-todos.html`

HEAD and the working tree compile and `mvn -o test` passes (58 tests, 1 skipped).
Everything the earlier audit listed as fixed — argument passing, the capacity-aware manager,
the S3 summary upload/download, HTML generation, worker bootstrap, the SQS visibility timeout,
and the SDK 2.57.0 BOM — has been removed from this document. The working tree still carries
uncommitted fixes to `Manager`, `App`, `AWS` and `Worker`; commit them before building the
submission.

Source of truth for anything below: `Assignment 1.pdf`.

## Open — bugs

1. **The client's wait is unbounded.** The manager now answers a failed `NEW_TASK` with an
   error `DONE_TASK`, but `App.requestTask` and `App.await` still have no deadline and the
   busy-retry path has no attempt cap. A lost reply or a dead manager hangs the client
   forever. Add a bounded wait and a retry policy.
2. **A finished client never gets its `TERMINATE` ack.** The client sends `TERMINATE` after
   `DONE_TASK` and waits for the ack; `Manager.finish` removes the client from `clientTasks`,
   so `handleTerminate` sends no ack to it and the client hangs. Ack the requester even when
   it is no longer in `clientTasks`.
3. **Error lines are not rendered as error lines.** A worker reports a failed image as
   `text = "exception: …"` with `isError = true`; `Manager.handleDoneOcr` stores only the
   text and drops the flag, while `App.generateHtmlFromSummary` only treats text starting
   with `ERROR:` as an error. The failed image therefore renders as an `<img>` plus text
   instead of `<link> <description>`. Prefix the summary text with `ERROR:` (or propagate
   the flag) and keep the client's format.
4. **Worker death recovery is only partly proven.** `SqsQueue` now sets a 300 s visibility
   timeout on first use (so `AWS.createQueue` itself still creates with the 30 s default),
   and release-on-failure exists. Still missing: a real kill/expiry test (the existing test
   calls `release()` manually), an assertion that the 300 s value is applied, and a
   heartbeat for OCR that runs longer than 300 s.
5. **The "offline" suite still touches AWS.** `WorkerTest.realImageOcrSmokeTest` is now
   `@Disabled`, but `ManagerTest.nonDevReadUrlsReadsFromS3` flips `Queues.setDev(false)` and
   triggers real EC2 calls (expired-credential warnings, ~50 s). Fake the AWS clients or gate
   that test so `mvn -o test` is clean and fast.
6. **An empty instance ID bypasses the UUID fallback.** `App` reads `$INSTANCE_ID`; if the
   metadata call fails it passes `""`, which is length 3, so `Manager.main` skips the UUID
   fallback and `setFreeSlots` no-ops on an empty ID. Guard the empty value.

## Open — assignment requirements

- **The manager is serial.** `Manager.run` handles `NEW_TASK` inline in one thread (S3 read,
  fan-out, summary write) and only polls done-OCR after the request batch. The PDF requires
  simultaneous processing of client requests: admit in the loop, submit per-client work to a
  bounded executor, and add an overlapping-clients test.
- **Terminate is not a drain.** `handleTerminate` clears `running`, sends `TERMINATE` to
  admitted clients, deletes the done-OCR queue immediately and still runs the current batch;
  it does not wait for outstanding OCR, send outstanding summaries, terminate the workers, or
  terminate its own instance. The PDF requires all four, with a bounded wait and a forced stop
  as fallback. (The missing ack for a finished client is bug 2.)
- **No persistence or restart model.** `clientTasks` is an in-memory map; a restarted manager
  loses every admitted client and ignores their later `DONE_OCR`. `NEW_TASK` is idempotent
  only while the client is live. Add a per-client S3 checkpoint and reload path, and keep
  `NEW_TASK` idempotent across restarts. (The client-side timeout is bug 1.)
- **S3/queue lifecycle is incomplete.** The manager now uploads summaries, but nothing deletes
  the `assignment1/` prefix or the shared `manager_queue`/`worker_queue`; only the done-OCR
  queue is deleted on shutdown. Add S3 list/delete helpers and verify the bucket and queues
  are empty after a run.
- **`ensureWorkers` is untested and sizes from the outstanding count.** `workersNeeded` is
  unit-tested, but `ensureWorkers` has no test and uses the outstanding URL count instead of
  the SQS depth. That is equivalent here, but the javadoc does not say so. Add a test and the
  note.
- **README is absent** (mandatory): names/ids, run instructions, design explanation,
  instance type/AMI, elapsed time and the `n` used, queue topology and the four message
  types, security notes.
- **The submission is not prepared:** zip the sources and class files (without downloaded
  libraries) plus the output of a run on the sample input.
- **Real-AWS verification is pending.** The credentials file exists but the session token is
  expired. Refresh the Learner Lab credentials (~4 h), confirm bucket `only-together2`, AMI
  `ami-00e95a9222311e8ed` (us-east-1), key pair `vockey` and `LabInstanceProfile`, then run
  `AwsIntegrationIT` and the end-to-end sample. Document the 19-instance maximum, "all
  workers busy" with more tasks than one worker can take, that the AMI has `java` and `aws`,
  and that the manager/worker instances really stop. `Manager.MAX_INSTANCES` is 19, but its
  javadoc still says 20.
- **Dead code:** `FreeSlotTags.chooseFreeManager` has no production caller (only tests), and
  the sort in `AWS.freeManagers()` has no consumer (the client only tests emptiness).
