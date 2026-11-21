# DSP Assignment 1 — Distributed OCR Pipeline on AWS

A distributed, asynchronous image-to-text (OCR) processing system built with Java, Maven, Amazon S3, Amazon SQS, Amazon EC2, and Tess4J (Tesseract OCR).

## Submission metadata (fill before submitting)

- **Names / IDs:** TODO
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

- **AMI ID**: `ami-0046c29ee3573ac7f` — the team's custom OCR image, `us-east-1` (configured
  in `src/main/java/utils/AWS.java`).
- **Native OCR Stack**: the image is an Amazon Linux AMI (`2018.03`) pre-provisioned with
  `libwebp`, Leptonica, Tesseract and `eng.traineddata`, plus Java 8 and the AWS CLI. The
  user-data scripts download the jars to `/home/ec2-user/` and run them without installing
  anything.
- **Availability**: the AMI must be shared with the account or copied into it; if it is not
  visible, EC2 reports `Not authorized for images` and no nodes start.
- **Local build tools**: JDK 17 and Apache Maven (`mvn`).

## Prerequisites & Configuration

1. **AWS Credentials**: Configure valid AWS credentials (`~/.aws/credentials` or environment variables) with permissions for **EC2**, **S3**, and **SQS** in `us-east-1` (such as AWS Learner Lab session credentials).
2. **SSH Key Pair**: Ensure your EC2 key pair (e.g., `~/.ssh/vockey.pem`) is available if connecting to EC2 instances for inspection or debugging.
3. **AMI Verification**: Confirm `public static String ami` in `src/main/java/utils/AWS.java`
   points at the team OCR AMI and that it is available in your account/region.

## Build & Run Tests

Compile the project and execute the offline test suite from the repository root:

```bash
mvn -o test
```

The suite needs no AWS credentials. 75 tests pass; the network OCR smoke test is skipped, and
so is the local Tess4J test when native Tesseract is not installed. To run the real OCR test
locally, install `tesseract-ocr` + `tesseract-ocr-eng` (or point `LD_LIBRARY_PATH` and
`TESSDATA_PREFIX` at an extracted copy).

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
worker) took about **3 min 50 s** from `NEW_TASK` to `DONE_TASK` on the Ubuntu boot-install
path. The pinned team AMI already contains the OCR stack, so boot should be faster; re-measure
before submitting. One URL OCRed successfully; two sample URLs were dead (404 / DNS) and
correctly rendered as error lines. `scripts/lab-run.sh` runs this small case end to end and
cleans up the instances, queues and S3 objects afterwards.