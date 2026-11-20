# PR #3 merge — open items

Date: 2026-11-18, audited again 2026-11-20
Baseline: `2f7fe2d` (`feature/manager-args-worker-bootstrap`) plus the working tree.
Rendered: `docs/superpowers/plans/2026-11-18-open-items-and-todos.html`

HEAD and the working tree compile and `mvn -o test` passes (53 tests). The former
working-tree compile error (an uncommitted `/newreadUrls` typo in `Manager.readUrls`) was
reverted on 2026-11-20; it was never committed, so the PR branch was not affected. Everything that the merge audit listed as done — argument passing, the
capacity-aware manager, `AWS.getS3()`, the `pending` filter, input validation, HTML
generation, worker bootstrap, worker delete/release — has been removed from this document.

Source of truth for anything below: `Assignment 1.pdf`.

## Open — bugs

1. **Manager ignores its instance ID.** `App.createEC2` passes `$INSTANCE_ID` as
   `args[2]`, but `Manager.main` parses only `args[0..1]` and builds itself with a random
   UUID. `setFreeSlots(managerID, …)` therefore tags a non-existent resource, so the `free`
   tag keeps the launch value forever and client-side availability is wrong.
   Identity decision: the EC2 instance ID is the manager identity in production (the UUID
   only for dev/tests).
2. **Workers never download the jar.** `Manager.launchWorkers` copies
   `s3://<bucket>/Worker.jar`; `App` uploads `assignment1/Worker.jar` (`AWS.S3_PREFIX`).
   The boot script fails and no worker runs.
3. **Instance cap is 20, not 19.** `Manager.MAX_INSTANCES = 20`; the PDF says "don't run
   more than 19 instances", manager included.
4. **Manager never touches S3 for the summary.** `finish()` writes
   `<inputFileLocation>.summary` locally (in production the input location is the S3 key,
   so the parent directory does not exist) and reports that path; the client downloads it
   from S3. The manager must download the input from S3 and upload the summary, returning
   the S3 key (local path only in dev).
5. **The client builds HTML from an empty file.** `App.execute` creates a temp file,
   downloads the summary into `outputFileName`, then parses the temp file and overwrites
   `outputFileName` with the resulting (empty) HTML. Download into the temp path instead.
6. **A failed `NEW_TASK` hangs the client.** `Manager.handle` deletes the request in
   `finally` even when handling threw, without replying; `App.await` polls without a
   timeout. Send an error `DONE_TASK`, and bound the client's wait.
7. **A worker that dies mid-message is not proven recoverable.** Deletion after a
   successful reply exists, but no queue visibility timeout is configured and no test
   kills a worker between receive and delete. Set a visibility timeout longer than OCR.
8. **Offline suite reaches the network.** `WorkerTest.realImageOcrSmokeTest` downloads a
   real image and runs OCR; it must not be part of `mvn -o test`.
9. **No AWS credentials / no real-AWS test.** Machine has no `~/.aws` or `AWS_*` vars; get Learner Lab key, secret and session token (expire ~4h). Verify bucket `only-together2`, AMI `ami-00e95a9222311e8ed` (us-east-1), `vockey`, `LabInstanceProfile` exist. Start with a free S3/SQS smoke test, then end to end.
10. **Separate AWS integration tests.** Put them in `*IT` tests that run only when credentials are present, never in `mvn -o test`; use throwaway bucket/queues and clean up.

## Open — assignment requirements

- **Manager is serial.** One receive loop handles `NEW_TASK` inline (S3 read, fan-out,
  summary write). The PDF requires simultaneous processing of client requests: admit in
  the loop, submit the per-client work to a bounded executor, add an overlapping-clients
  test.
- **Terminate is not a drain.** `handleTerminate` clears `running`, sends `TERMINATE` to
  admitted clients and deletes the done-OCR queue; it does not wait for outstanding work,
  terminate the workers, or terminate its own instance. The PDF requires all four, with a
  bounded wait and forced stop as fallback.
- **No persistence or retry model.** `clientTasks` is in memory only; a restarted manager
  loses every admitted client, and a client never times out on a dead manager. Minimum: a
  per-client S3 checkpoint plus a client-side timeout/retry, keeping `NEW_TASK` idempotent.
- **S3 lifecycle incomplete.** Prefix and public-read ACL exist, but the manager does not
  upload, nothing deletes the prefix or the `manager_queue`/`worker_queue`, and nobody
  verifies the bucket and queues are empty afterwards.
- **Worker count is untested.** `ensureWorkers` uses the outstanding URL count instead of
  the SQS depth (equivalent here — document it), applies `m - k` and the cap, but
  `workersNeeded` has no unit test and the 19-instance cap is wrong (item 3).
- **README absent** (mandatory): names/ids, run instructions, design explanation,
  instance type/AMI, elapsed time and the `n` used, queue topology and the four message
  types, security notes.
- **Submission not prepared:** zip the sources/class files (without downloaded libraries)
  plus the output of a run on the sample input.
- **SDK version deviates:** BOM `2.18.16` vs the assignment's `2.57.0`; bump it or document
  the deviation in the README.
- **Verified-run checks:** 19 instances maximum and "all workers busy" with more tasks than
  one worker can take; confirm the AMI has `java` and `aws` (both boot scripts depend on
  them) and that the manager/worker instances really stop.
- **Dead code:** `AWS.chooseFreeManager` has no caller, and the sort in `freeManagers()`
  is unused (the client only tests emptiness).
