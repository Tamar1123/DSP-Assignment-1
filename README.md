# DSP Assignment 1 — Distributed OCR Pipeline on AWS

A distributed, asynchronous image-to-text (OCR) processing system built with Java, Maven, Amazon S3, Amazon SQS, Amazon EC2, and Tess4J (Tesseract OCR).

## Architecture Overview

The system decouples client submission, task orchestration, and OCR execution across AWS services and dedicated application components:

- **LocalApp (`client`)**: Accepts input image URL lists, uploads artifacts and input files to Amazon S3, submits jobs to the Manager request queue, and polls the client's SQS queue for completion or error notifications before writing the final summary output.
- **Manager (`server.Manager` — `Router` & `IoWorker`)**: Coordinates active client tasks (`ClientTask`) across dedicated routing and I/O worker threads, fans out image URLs to OCR work queues, tracks per-client completion (`outstanding` / `done`), preserves input URL ordering when aggregating recognized text, uploads summary files to S3, and manages worker instance scaling and termination.
- **Worker (`server.Worker`)**: Polls image tasks from SQS, downloads target images, performs OCR using **Tess4J** backed by native Tesseract, Leptonica, and `libwebp` libraries with English trained data (`eng.traineddata`), and publishes OCR results or formatted error reports back to the completion queue.
- **AWS Transport & Storage (`utils.AWS`, `utils.Queue`, `utils.Queues`)**: Uses Amazon S3 for input/summary storage and Amazon SQS (supporting batched message sends and bounded client timeouts) for asynchronous communication, alongside an in-memory queue mode (`dev`) for local execution and automated unit testing.

## AMI & Environment Setup

- **Custom AMI ID**: `ami-0046c29ee3573ac7f` (configured in `src/main/java/utils/AWS.java`).
- **Base OS & Native OCR Stack**: Amazon Linux AMI (`2018.03`) pre-provisioned with compiled `libwebp`, `leptonica`, `tesseract`, and `eng.traineddata` so EC2 `Worker` instances launched dynamically by `Manager` can run Tess4J out of the box without runtime compilation.
- **Runtime & Build Tools**: Java 8 (`openjdk 1.8.x`) and Apache Maven (`mvn`).

## Prerequisites & Configuration

1. **AWS Credentials**: Configure valid AWS credentials (`~/.aws/credentials` or environment variables) with permissions for **EC2**, **S3**, and **SQS** in `us-east-1` (such as AWS Learner Lab session credentials).
2. **SSH Key Pair**: Ensure your EC2 key pair (e.g., `~/.ssh/vockey.pem`) is available if connecting to EC2 instances for inspection or debugging.
3. **AMI Verification**: Confirm `public static String ami = "ami-0046c29ee3573ac7f";` is set in `src/main/java/utils/AWS.java`.

## Build & Run Tests

Compile the project and execute the test suite from the repository root:

```bash
mvn test
```

## Running the Application

Package the application JAR with Maven:

```bash
mvn clean package
```

Execute `LocalApp` (or `Manager` / `Worker` in standalone/dev modes) with the required command-line arguments (`inputFileName`, `outputFileName`, `n` workers-per-task ratio, and optional `terminate` flag) according to the assignment specification.