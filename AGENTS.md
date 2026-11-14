# AGENTS.md

Assignment 1 (DSP): **OCR in the Cloud**. A local application (client) hands an input file of
image URLs to a *manager* on EC2; the manager spreads the URLs across *worker* nodes, which OCR
the images and report text back; the manager writes one summary per client to S3 and tells the
client; the client turns it into an HTML file. Grading is frontal: everything below is checked,
so treat it as the requirement, not as background.

Source of truth: `Assignment 1 (1).pdf` in this directory. If this file and the PDF disagree,
the PDF wins — and say so.

## Run contract (fixed — the grader runs this)

```
java -jar yourjar.jar inputFileName outputFileName manager-clients-n worker-files-n [terminate]
```

| argument | meaning |
|---|---|
| `inputFileName` | input file: image URLs, **one per line** |
| `outputFileName` | output HTML file |
| `manager-clients-n` | maximum clients per manager |
| `worker-files-n` | workers' files ratio (max files per worker) |
| `terminate` | optional: at the end, send a terminate message to the manager |

Parsed in `client.App.main` into static fields. Keep the four positional arguments and their
order exactly as above. `-D` is an undocumented dev switch (debug output, keep the queue).

`java -jar` is part of the contract, so the jar must have a `Main-Class` (`client.App`) and
carry its dependencies (shade) — today it has neither.

## Flow the assignment describes

1. Local app reads the input file (URLs, one per line).
2. It checks whether a manager node is available — **active and serving fewer than
   `manager-clients-n` clients** — and starts one if not.
3. It uploads the input file to S3.
4. It sends a *new task* message (the S3 location of the file) to the manager's SQS queue.
5. Manager downloads the input from S3 and creates one *new image task* message per URL.
6. Manager bootstraps worker nodes: one worker per `worker-files-n` files if none are running;
   with `k` already active and `m` needed, it starts `m - k` more if possible.
7. A worker takes a message, downloads the image, OCRs it, sends the text back to the manager,
   deletes the message.
8. When every URL is done, the manager writes one summary file per client, uploads it to S3 and
   sends a *done* message carrying its S3 location.
9. Local app downloads the summary and writes the HTML output: each image plus its text, linked
   to the original image URL. On an exception or a missing image the line is
   `<link to the input file> <short description of the exception>`.
10. In `terminate` mode the local app then sends a terminate message to the manager.

## Message types (assignment's names)

| message | direction | payload |
|---|---|---|
| new task | application → manager | location of the input file in S3 |
| new image task | manager → workers | URL of one image |
| done OCR | worker → manager | image URL + identified text |
| done | manager → application | S3 location of the summary file |

`utils.Message` already models these (`NEW_TASK`, `NEW_IMAGE_TASK`, `DONE_OCR`, `DONE_TASK`,
`TERMINATE`) plus `localAppID` and Jackson `toJson` / `fromJson`. It is the only wire format.

## Layout

```
src/main/java/client/App.java        local application: arg parsing, S3 upload, queue, HTML output
src/main/java/server/manager.java    manager, runs on EC2 (currently an empty class)
src/main/java/utils/AWS.java         singleton: S3 / SQS / EC2 clients, bucket, instance, queue helpers
src/main/java/utils/Message.java     wire format (Jackson JSON)
src/test/java/AppTest.java           JUnit 5
codeExamples/                        separate Maven project: AWS snippets + sample input/output
Assignment 1.pdf                     the assignment
docs/superpowers/specs/              tamar's design notes and implementation plans (gitignored, local only)
```

## Build and test

```
mvn -o test        # unit tests; must pass offline and without AWS credentials
mvn -o package     # -> target/LocalApp-1.0-SNAPSHOT.jar
```

Java 8 source level (`maven.compiler.source/target`), so no `var`, records, switch expressions,
`List.of`, text blocks. AWS SDK v2 (`software.amazon.awssdk`), Jackson, slf4j, JUnit 5.

The assignment's own dependency list says SDK `2.57.0`; this pom imports the `software.amazon.awssdk:bom`
at `2.18.16` (property `aws.java.sdk.version`). Never hardcode a version for an SDK artifact —
change the BOM property if it has to move.

## Rules from the assignment that shape the design

- **All workers share one queue.** The manager must *not* hand messages to specific workers; any
  worker may take any message. Consequence: with 2n messages and 2 workers, one may do n+n/2.
- **The manager processes client requests in parallel**, not one at a time.
- **Visibility timeout matters.** A worker that dies mid-message must not take the task with it
  — leave the message undeleted and let the visibility timeout return it to the queue.
- **Workers recover.** On an exception the worker sends the manager that image's message plus a
  short description, then carries on with the next message.
- **Terminate** = manager stops accepting new clients, waits for workers to finish, terminates
  them, sends any outstanding responses, then terminates itself.
- **Several local applications run at once.** Per the notes, race conditions such as two clients
  starting a manager simultaneously may be assumed away.
- The system is graded on parallelism, time, cost, scalability, persistence and failure
  handling — "awaiting another component" is called out as *not* distributed.
- Manager/worker responsibilities must not be mixed: the manager is not a worker.

## AWS settings already encoded

- S3 and SQS: `us-west-2` (`AWS.region1`); EC2: `us-east-1` (`AWS.region2`).
- Bucket: `AWS.bucketName` (`only-together2`), AMI `ami-00e95a9222311e8ed`, key pair `vockey`,
  `LabInstanceProfile`, `M4_LARGE` — see `utils/AWS.createEC2`.
- No credentials on this machine. **Never** write a test that reaches AWS; keep decision logic
  free of SDK types so it can be tested against a fake.
- AMIs may be disabled for students: then the user-data script must download the jars each
  boot. User data must be base64-encoded (the existing `createEC2` does this).
- Put everything in one bucket folder so it can all be deleted; files must be public to be
  downloadable. Verify nodes really stopped — the manager must stop workers, but check yourself.
- Credentials are sensitive: never a plain-text credential in the repo, the jar or the zip.

## Design decisions in this repo (agreed — not all written yet)

- **One fixed manager queue plus one reply queue per client.** `manager_queue` is shared: clients
  send `NEW_TASK` and `TERMINATE` there, any manager reads it. Replies (`DONE_TASK`, the
  `TERMINATE` ack) go to `client_<localAppID>`, which the client creates; the manager derives the
  name from the `localAppID` in the request. Planned `AWS.managerQueueName()` /
  `AWS.clientQueueName(id)`. *(not written yet)*
- Each queue has exactly one kind of writer, so messages need no sender field; the queue is the
  separator. Do not re-add a `Sender` field without a reason a queue split cannot cover.
- `utils.Received` + a small `utils.Queue` interface (`send`/`receive`/`delete`/`release`) keep
  the loops testable without AWS; releasing (visibility 0) rather than dropping matters, an
  unread-but-received message is invisible for the visibility timeout. *(not written yet)*
- `terminate` is sent **after** `DONE_TASK`, never before: ordering is not guaranteed, and a
  manager that stops accepting early never answers.
- Full reasoning: `docs/superpowers/specs/2026-11-14-sqs-queue-sync-design.md` (gitignored).

## Known gaps and conflicts

- `manager-clients-n` availability ("running and serving fewer than N clients") is defined as an
  EC2 `clients` tag the manager keeps current and the client reads; tags are not strongly
  consistent, so the count is approximate. Not implemented.
- `server/manager.java` is empty: no manager logic at all.
- `utils/AWS.java` has only `createBucketIfNotExists`, `createEC2`, `createSqsQueue(String)` —
  no queue URL lookup, send, receive, delete or release; `createSqsQueue` returns nothing.
- `client.App` only creates the bucket and starts an instance whose user data is
  `echo Hello World`. No S3 upload/download, no SQS, no wait loop, no HTML output.
- No workers, no OCR, no task splitting, no `worker-files-n` / `manager-clients-n` logic, no
  availability check for a running manager, no tags to find manager instances.
- No OCR dependency on the classpath: `com.asprise.ocr:java-ocr-api` is only in
  `dependencyManagement`. Assignment-suggested options: Tesseract (`net.sourceforge.tess4j`,
  5.20.0), javaOCR (`net.sourceforge.javaocr:javaocr-core`, 1.0), Asprise (15.3.0.3).

## Conventions

- Packages stay lowercase (`client`, `server`, `utils`); `manager` is lowercase for historical
  reasons — do not rename without asking.
- Get AWS clients from the `utils.AWS` singleton; do not build new `S3Client` / `SqsClient` /
  `Ec2Client` elsewhere.
- Do not add dependencies without asking.
- Keep `[DEBUG]` output behind the `debug` flag, as it is today.

## Gotchas

- Git still shows the old flat `src/main/java/{App,AWS,Message}.java` as deleted; they live in
  `client/` and `utils/` now.
- `codeExamples/` is a separate Maven project (its own pom, not a module): untouched AWS
  snippets and the sample `inputExample.txt` / `outputExample.html`. Reference, not used code.
- `myapp/` next to this repository is an unrelated AWS Lambda template.
- `target/assignment1-project-1.0-SNAPSHOT.jar` is stale (before the artifactId became
  `LocalApp`); this build produces `target/LocalApp-1.0-SNAPSHOT.jar`.
- `docs/` is gitignored.

## Submission checklist (from the assignment)

- Zip: all sources and class files (without the libraries students download), plus the output of
  a run on the sample input file.
- `README` (mandatory, graded): names and ids, how to run it, how it works, the time the run
  took on the sample file, and the `n` used.
- Be ready to explain: security, scalability, persistence, failures, threads, multiple
  concurrent clients, termination, queue choice, who does what.
