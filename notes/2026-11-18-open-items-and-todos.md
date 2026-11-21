# Open items and TODOs

Date: 2026-11-18, re-audited 2026-11-21 after merging `origin/master` (`8c22d57`).
Baseline: HEAD `8c22d57` plus the working tree.
Rendered: `notes/2026-11-18-open-items-and-todos.html`

## Where things stand

- `origin/master` was pulled (fast-forward from `dc2f18e`). It brings the Tess4J migration,
  the `ERROR:` prefix for failed images, the empty-instance-id guard and the README. The only
  conflict was `Manager.handleDoneOcr`; the merge keeps both sides (error prefix plus the
  outstanding-count debug log).
- `mvn -o test` is green and never contacts AWS: 75 tests, 0 failures, 2 skipped. The skips are
  the disabled network OCR smoke test and the new local OCR test when native Tesseract is not
  installed. The ~85 s runtime is `InMemoryQueue` long-poll sleeps
  (`RECEIVE_WAIT_SECONDS = 1`), not AWS calls.
- **The new OCR engine works.** Locally, Tess4J 5.11.0 with native Tesseract 4.1.1 read a
  generated "HELLO 12345" image (`WorkerTest.tess4jReadsAGeneratedImage`). On EC2, a full run
  with `manager-clients-n=1`, `worker-files-n=100` and 3 URLs produced one real OCR line and
  two error lines for the dead sample URLs, then sent `TERMINATE` and received the ack.
  Client start to `DONE_TASK` was ~3 min 50 s, most of it package installation at boot.
- The AMI committed in `cfc08e3` (`ami-0046c29ee3573ac7f`) **does not exist in the current
  Learner Lab account** ("Not authorized for images"). The code now pins Ubuntu 22.04 LTS
  `ami-00b13f11600160c10` (us-east-1), and both node scripts install
  `openjdk-17-jre-headless tesseract-ocr tesseract-ocr-eng awscli` in user-data before starting.
- Everything the earlier audit listed as fixed stayed fixed: argument passing, the capacity-aware
  manager, the S3 summary upload/download, HTML generation, worker bootstrap, the SQS visibility
  timeout, the SDK 2.57.0 BOM, the `Router`/`IoWorker` split, and the `TERMINATE` ack for a
  client that already finished.

## Open — bugs

1. **The client's wait is unbounded.** `App.requestTask` and `App.await` still have no deadline
   and the busy-retry path has no attempt cap. A lost reply or a dead manager hangs the client
   forever. Add a bounded wait and a retry policy. (The manager already answers a failed
   `NEW_TASK` with an error `DONE_TASK`, so the failure path exists; only the timeout is missing.)
2. **Worker death recovery is only partly proven.** `SqsQueue` sets a 300 s visibility timeout
   on first use and release-on-failure exists, but there is no test that kills a worker or waits
   for expiry, no assertion that the 300 s value is actually applied, and no heartbeat for an
   OCR call that runs longer than 300 s.

## Open — assignment requirements

- **Terminate is a drain, not a full shutdown.** The manager now stops admitting, waits for
  `clientTasks` to empty, acks the requester (even if it already finished) and deletes the
  done-OCR queue. It still does **not** terminate workers, terminate its own instance, or bound
  the drain with a forced stop. If a worker dies, `clientTasks` never empties and the manager
  waits forever. The successful lab run was cleaned up by `scripts/lab-run.sh`, not by the
  manager.
- **No persistence or restart model.** `clientTasks` is an in-memory map; a restarted manager
  loses every admitted client and ignores their later `DONE_OCR`. `NEW_TASK` is idempotent only
  while the client is live. Add a per-client S3 checkpoint and reload path, and keep `NEW_TASK`
  idempotent across restarts.
- **S3/queue lifecycle is incomplete.** The manager uploads summaries, but nothing deletes the
  `assignment1/` prefix or the shared `manager_queue`/`worker_queue`; only the done-OCR queue is
  deleted on shutdown. Add S3 list/delete helpers and verify the bucket and queues are empty
  after a run.
- **`ensureWorkers` is untested and sizes from the outstanding count.** `workersNeeded` is
  unit-tested and the offline test now overrides `ensureWorkers`, but the method still has no
  test of its own. It uses the outstanding URL count instead of the SQS depth; that is
  equivalent here, but the javadoc does not say so.
- **README exists but is incomplete for grading.** It now needs the mandatory items: names and
  ids, exact run command (`java -jar target/App.jar ...`), the `n` used, the measured run time,
  queue topology and the four message types, and the security notes. Its old "custom tesseract
  AMI" section has been replaced by the Ubuntu bootstrap.
- **The submission is not prepared:** zip the sources and class files (without downloaded
  libraries) plus the output of a run on the sample input.
- **Real-AWS verification is partial now that credentials are valid.** The bucket
  `dsp1-only-together2-654654248213` exists, `vockey` and `LabInstanceProfile` exist, and the
  Ubuntu AMI launches. Still unverified: the 19-instance maximum, "all workers busy" with more
  tasks than one worker can take, that the AMI has everything after a Learner Lab reset (the
  user-data install is now the recovery path), and that the manager itself stops workers and
  its own instance. The sample `inputExample.txt` has only one live URL out of the first three
  (one 404, one DNS failure), so a sample run legitimately contains error lines.
- **Dead code:** `FreeSlotTags.chooseFreeManager` has no production caller (only tests), and
  the sort in `AWS.freeManagers()` has no consumer (the client only tests emptiness).

## Follow-ups

- **A second `TERMINATE` during the drain gets no reply.** Today a duplicate `TERMINATE` while
  `terminating` is already true is deleted without a reply: `Manager.handleTerminate` returns
  on the failed CAS, and `handle` still deletes the message. The sender must receive the error
  **"Manager already started terminating"**. Transport decision needed: the sender is blocked
  in `App.await(..., TERMINATE)`, which only consumes `TERMINATE`-type messages and releases
  everything else, so an error `DONE_TASK` would be ignored and redelivered. Either (a) reply
  with a `TERMINATE` message carrying `isError = true` and the text in `outputFileLocation`,
  and have `App.run` surface it, or (b) teach `await` to treat an error `DONE_TASK` as terminal
  while waiting for `TERMINATE`. Under the single-terminate assumption this is the
  duplicate/at-least-once path, not a second client.
- **Boot cost is now the dominant latency.** Each node runs `apt-get update` plus four package
  installs, ~2–3 minutes per run. Bake those packages into a custom AMI (update `AWS.ami` and
  the README) if the measured time matters for grading.
- **Local OCR needs native Tesseract.** `WorkerTest.tess4jReadsAGeneratedImage` skips when
  `libtesseract` is absent. Install `tesseract-ocr` + `tesseract-ocr-eng` (or set
  `LD_LIBRARY_PATH`/`TESSDATA_PREFIX` to an extracted copy) to make it run locally.
