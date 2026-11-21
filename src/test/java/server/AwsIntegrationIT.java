package server;

import org.junit.jupiter.api.*;
import utils.*;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

// AWS Learner Lab integration smoke test for the Manager and Worker.
// Prerequisites:
// 1. Get Learner Lab credentials from the lab console:
//      export AWS_ACCESS_KEY_ID=...
//      export AWS_SECRET_ACCESS_KEY=...
//      export AWS_SESSION_TOKEN=...
//    They expire after ~4 hours.
// 2. Verify the bucket "dsp1-only-together2-654654248213" (us-west-2) and AMI
//    "ami-00e95a9222311e8ed" (us-east-1) still exist.
// 3. This test does NOT use the client: it sends messages directly to the manager queue and
//    reads replies from a client queue it creates itself.
// Run with: mvn test -Dtest=AwsIntegrationIT -Dgroups=integration
// Never runs under "mvn -o test".
@Tag("integration")
public class AwsIntegrationIT {

    private static final String TEST_ID = "it-" + UUID.randomUUID().toString().substring(0, 8);
    private static String clientQueueName;
    private static String s3InputKey;
    private static String s3SummaryKey;

    @BeforeAll
    static void checkCredentials() {
        Assumptions.assumeTrue(
                System.getenv("AWS_ACCESS_KEY_ID") != null,
                "Skipping integration test: no AWS credentials");
    }

    @BeforeEach
    void setUp() throws Exception {
        Queues.setDev(false);
        clientQueueName = Queues.clientQueueName(TEST_ID);

        // Create the client reply queue and a one-URL input file on S3.
        Queues.create(clientQueueName);

        File input = File.createTempFile("it-input-", ".txt");
        input.deleteOnExit();
        PrintWriter w = new PrintWriter(input);
        w.println("https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png");
        w.close();

        s3InputKey = AWS.S3_PREFIX + "input-files/" + TEST_ID + "-input.txt";
        AWS.getInstance().getS3().putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(AWS.getInstance().bucketName)
                        .key(s3InputKey)
                        .acl(software.amazon.awssdk.services.s3.model.ObjectCannedACL.PUBLIC_READ)
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromFile(input));

        s3SummaryKey = AWS.S3_PREFIX + "summaries/" + TEST_ID + ".txt";
    }

    @AfterEach
    void tearDown() {
        // Clean up S3 objects and the client queue.
        try {
            AWS.getInstance().getS3().deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
                    .bucket(AWS.getInstance().bucketName).key(s3InputKey).build());
            AWS.getInstance().getS3().deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
                    .bucket(AWS.getInstance().bucketName).key(s3SummaryKey).build());
        } catch (Exception ignored) { }
        try {
            String url = AWS.getInstance().createQueue(clientQueueName);
            AWS.getInstance().deleteQueue(url);
        } catch (Exception ignored) { }
        Queues.reset();
    }

    @Test
    public void managerProcessesNewTaskAndWorkerOcrsIt() throws Exception {
        // Send NEW_TASK directly to the manager queue (no client code involved).
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask(s3InputKey, TEST_ID));

        // Poll the client reply queue for DONE_TASK (3-minute timeout).
        long deadline = System.currentTimeMillis() + 180_000L;
        Message done = null;
        while (System.currentTimeMillis() < deadline) {
            List<Received> received = Queues.get(clientQueueName).receive(1, 20);
            for (Received r : received) {
                if (r.getMessage().getType() == Message.MessageType.DONE_TASK) {
                    done = r.getMessage();
                    break;
                }
            }
            if (done != null) break;
        }

        Assertions.assertNotNull(done, "DONE_TASK should arrive within 3 minutes");
        Assertions.assertFalse(done.isError(), "DONE_TASK should not be an error: "
                + done.getOutputFileLocation());
        Assertions.assertEquals(s3SummaryKey, done.getOutputFileLocation(),
                "summary location should match the expected S3 key");

        // Download the summary and verify content.
        Path local = Files.createTempFile("it-summary-", ".txt");
        AWS.getInstance().getS3().getObject(
                software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                        .bucket(AWS.getInstance().bucketName)
                        .key(s3SummaryKey).build(),
                software.amazon.awssdk.core.sync.ResponseTransformer.toFile(local));
        String content = new String(Files.readAllBytes(local));
        Assertions.assertTrue(content.contains("http"),
                "summary should contain the image URL");
        Assertions.assertTrue(content.contains("\t"),
                "summary should have tab-separated URL and text");
    }
}
