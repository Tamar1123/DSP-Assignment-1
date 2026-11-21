# DSP Assignment 1 — Distributed OCR Pipeline on AWS

A distributed, asynchronous image-to-text (OCR) processing system built with Java, Maven, Amazon S3, Amazon SQS, Amazon EC2, and Tess4J (Tesseract OCR).

## Submission metadata (fill before submitting)
- **Written by:** Nir Bergman (322974635) and Tamar Shefi Simchon (212550966)
- **Instances:** manager and workers both use AMI `ami-0e72706534bb166e3` (Ubuntu 22.04, `us-east-1`), type **m4.large**, `LabInstanceProfile`, key pair `vockey`.
- **Run time:** ~3 min 50 s on the 3-URL sample (see *Running the Application*); re-measure and update before submitting.
- **`n` used for the sample run:** `manager-clients-n=1`, `worker-files-n=100`

## Architecture Overview

The system decouples client submission, task orchestration, and OCR execution across AWS services and dedicated application components:

- **LocalApp (`client`)**: Accepts input image URL lists, uploads artifacts and input files to Amazon S3, submits jobs to the Manager request queue, and polls the client's SQS queue for completion or error notifications before writing the final summary output.
- **Manager (`server.Manager` — `Router` & `IoWorker`)**: Coordinates active client tasks (`ClientTask`) across dedicated routing and I/O worker threads, fans out image URLs to OCR work queues, tracks per-client completion (`outstanding` / `done`), preserves input URL ordering when aggregating recognized text, uploads summary files to S3, and manages worker instance scaling and termination.
- **Worker (`server.Worker`)**: Polls image tasks from SQS, downloads target images, performs OCR using **Tess4J** backed by native Tesseract, Leptonica, and `libwebp` libraries with English trained data (`eng.traineddata`), and publishes OCR results or formatted error reports back to the completion queue.
- **AWS Transport & Storage (`utils.AWS`, `utils.Queue`, `utils.Queues`)**: Uses Amazon S3 for input/summary storage and Amazon SQS (supporting batched message sends and bounded client timeouts) for asynchronous communication, alongside an in-memory queue mode (`dev`) for local execution and automated unit testing.

### Queue topology and messages

- `manager_queue` — shared request queue; every client sends `NEW_TASK` and `TERMINATE` here.
- `worker_queue` — shared worker queue; the manager sends `NEW_IMAGE_TASK`; any worker may take
  any message.
- `client_<localAppID>` — one reply queue per client, created by the client; the manager sends
  `DONE_TASK` and the `TERMINATE` ack here.
- `done_ocr_<managerID>` — one result queue per manager; workers send `DONE_OCR` here.

| message | direction | payload |
|---|---|---|
| `NEW_TASK` | application → manager | S3 location of the input file |
| `NEW_IMAGE_TASK` | manager → workers | URL of one image |
| `DONE_OCR` | worker → manager | image URL + identified text (or `isError`) |
| `DONE_TASK` | manager → application | S3 location of the summary file |

### Security notes

- No credentials are stored in the repository or the jars. Nodes use the attached
  `LabInstanceProfile`; the local app reads `~/.aws/credentials` or environment variables.
- The bucket is account-scoped (`dsp1-only-together2-<account-id>`) and the manager/worker
  instances are tagged (`Name=manager` / `Name=worker`) so they can be found and stopped.
- SQS messages carry no credentials; S3 keys are scoped under `assignment1/`.
- Learner Lab session credentials expire after ~4 hours; refresh them before a run.

## AMI & Environment Setup

- **AMI ID**: `ami-0e72706534bb166e3` — a public Ubuntu 22.04 image in `us-east-1`
  (configured in `src/main/java/utils/AWS.java`). It is visible in every account, so no
  custom image has to be shared or copied.
- **OCR is bundled, not installed**: Tesseract, Leptonica and their shared-library closure
  live in `src/main/resources/ocr-native/linux-x86-64/`, and the trained data comes from the
  tess4j jar. `utils.OcrRuntime` extracts both from the classpath on first use, loads the
  libraries and points Tess4J at them, so a node needs no `apt`/`yum` OCR install. If the
  bundled binaries cannot load (they are built on Ubuntu 22.04, so they need a glibc at least
  that new), the worker logs it and falls back to the image's own Tesseract installation.
- **Refreshing the native bundle**: `scripts/bundle-ocr-native.sh` re-downloads the Ubuntu
  jammy packages and rewrites `src/main/resources/ocr-native/linux-x86-64/` (the versions are
  recorded in that directory's `MANIFEST.txt`). Re-run it for another glibc baseline; the
  `.so` files must not require a newer glibc than the target image.
- **Local build tools**: JDK 17 and Apache Maven (`mvn`).

## Prerequisites & Configuration

1. **AWS Credentials**: Configure valid AWS credentials (`~/.aws/credentials` or environment variables) with permissions for **EC2**, **S3**, and **SQS** in `us-east-1` (such as AWS Learner Lab session credentials).
2. **SSH Key Pair**: Ensure your EC2 key pair (e.g., `~/.ssh/vockey.pem`) is available if connecting to EC2 instances for inspection or debugging.
3. **Image**: nothing to prepare — the default public Ubuntu 22.04 AMI is available in every
   account, and the OCR engine ships inside `Worker.jar`.

## Build & Run Tests

Compile the project and execute the offline test suite from the repository root:

```bash
mvn -o test
```

The suite needs no AWS credentials and no system Tesseract: the real OCR test runs against the
bundled native libraries. Only the network OCR smoke test is disabled. `TESSDATA_PREFIX` still
overrides the bundled trained data when set.

## Running the Application

Package the application JAR with Maven:

```bash
mvn -o clean package
```

This produces shadable, self-contained jars in `target/`: `App.jar` (the local application),
`Manager.jar` and `Worker.jar`.

The assignment contract:

```bash
java -jar target/App.jar <inputFileName> <outputFileName> \
    <manager-clients-n> <worker-files-n> [terminate] [-D]
```

- `inputFileName` — one image URL per line.
- `outputFileName` — the HTML result file.
- `manager-clients-n` — maximum clients per manager.
- `worker-files-n` — maximum image files per worker (one worker per this many URLs).
- `terminate` — after `DONE_TASK`, ask the manager to stop.
- `-D` — debug output; EC2 manager/worker logs are mirrored to
  `s3://<bucket>/assignment1/logs/`.

**Measured run:** `manager-clients-n=1`, `worker-files-n=100`, 3 sample URLs (1 manager + 1
worker) took about **3 min 50 s** from `NEW_TASK` to `DONE_TASK` on the earlier Ubuntu
boot-install path. The OCR install is no longer part of boot (it ships in the jar), so
re-measure before submitting. One URL OCRed successfully; two sample URLs were dead
(404 / DNS) and correctly rendered as error lines. `scripts/lab-run.sh` runs this small case end to end and
cleans up the instances, queues and S3 objects afterwards.

## How it works (step by step)

1. LocalApp reads the URL file, finds a manager with a free client slot (EC2 `Name=manager` tag plus free-slot tag) or starts one, uploads the input to S3, creates its own `client_<id>` queue and sends `NEW_TASK` to `manager_queue`.
2. The manager's Router thread admits the client (up to `manager-clients-n` concurrent) and hands the blocking work to its IoWorker: download the input from S3, send one `NEW_IMAGE_TASK` per URL (batched) to `worker_queue`, and start `ceil(URLs / worker-files-n)` workers minus those already running.
3. Workers take any message from the shared queue, download the image, OCR it (Tesseract), send `DONE_OCR` (text or an error description) to the manager's `done_ocr_<managerID>` queue, then delete the message.
4. The manager counts results per client; when all are in, it writes the summary (in input order) to S3 and sends `DONE_TASK` to `client_<id>`.
5. LocalApp downloads the summary and writes the HTML (image + text linked to the URL; failures as `<url> <short error>`). With `terminate`, it then sends `TERMINATE` (always after `DONE_TASK`).
6. On terminate the manager stops accepting clients, finishes outstanding ones, sends the replies, terminates exactly the workers it launched, waits until they stop, and terminates itself.

## Mandatory-requirements discussion

**Security.** No credentials in the repo, jars, zip, user data or messages. Nodes use `LabInstanceProfile`; the local app reads `~/.aws/credentials`/env vars. Messages carry only S3 keys/URLs/text. Everything lives under one bucket prefix `assignment1/` in an account-scoped bucket, so it can be deleted in one go.

**Scalability.** Each client has its own reply queue, so clients never contend for responses, and SQS/S3 scale horizontally. The manager is not a bottleneck by design: its Router only does non-blocking admission, the IoWorker does S3/SQS/EC2 calls, and the heavy OCR is done by workers. When a manager is full, LocalApp starts another (own `done_ocr_<id>` queue and worker set), so managers grow with clients and workers grow with images (`worker-files-n`). Honest limits: the EC2 instance quota of the account caps real parallelism; queues and S3 are not limits.

**Persistence and failures.**
- *Worker dies/stalls mid-task:* the message is deleted only after the result was sent, so after the visibility timeout it reappears and another worker takes it (at-least-once; duplicates are harmless). On an exception/404/bad image the worker reports an error result for that URL and continues; on shutdown it releases the message.
- *Worker instance crash:* a worker terminates its own instance when its JVM exits, so the manager sees it as `terminated`; every 10 s `reconcileWorkers` drops dead IDs and launches replacements while images remain outstanding.
- *Manager dies:* unhandled `NEW_TASK` messages stay in SQS and inputs/summaries in S3. Known gap: the in-memory worker set and per-client progress are lost (orphaned workers drain the queue and shut themselves down).
- *Broken communication:* bounded SQS/S3 timeouts, the Router loop catches exceptions so one bad batch cannot kill the node, and a failed task returns an error reply instead of silence.

**Threads.** The manager uses two threads: a Router (receive and admit, never blocks) and an IoWorker (all slow S3/EC2/SQS calls). Threads are good for I/O waits and bad for CPU-bound work and shared mutable state, so there is no thread per client/URL. OCR is CPU-bound, so parallelism comes from more worker instances. Shared state uses thread-safe collections/synchronized methods.

**Multiple clients.** Each client has a unique `localAppID` and its own reply queue; the manager tracks progress per client (`ClientTask`), so concurrent runs get only their own results.

**Termination.** `terminate` is sent after `DONE_TASK`, so a manager that stops accepting never leaves a client unanswered. The manager then terminates its workers, waits until they are gone, and terminates itself. Check the EC2 console afterwards.

**System limits.** SQS messages max 256 KB (we send only URLs/short text), batches of 10, long polling. Worker count is `ceil(images / worker-files-n)`, bounded by the EC2 quota.

**Are all workers busy?** All workers pull from one shared queue, so whoever is free takes the next image; nothing is pre-assigned. A worker idles only while booting or when the queue is empty.

**Separation of duties.** LocalApp: upload, request, render HTML. Manager: admission, split, scale workers, aggregate, summarise, terminate; never OCRs. Worker: download + OCR + report only.

**Distributed.** Components communicate only through queues and S3, so none waits on another being ready and each can be restarted. The only unavoidable wait is the client waiting for its own `DONE_TASK`.
