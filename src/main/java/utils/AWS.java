package utils;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static String ami = "ami-00e95a9222311e8ed";

    public static Region region1 = Region.US_EAST_1;
    public static Region region2 = Region.US_EAST_1;

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return InstanceHolder.INSTANCE;
    }

    private static final class InstanceHolder {
        private static final AWS INSTANCE = new AWS();
    }

    public String bucketName = "dsp1-only-together2-262171960391";
    public static final String S3_PREFIX = "assignment1/";


    // S3
    public void createBucketIfNotExists(String bucketName) {
      try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return;
        } catch (NoSuchBucketException e) {
            // Bucket does not exist yet; proceed to create it.
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                // Bucket not found; proceed to create it.
            } else {
                System.out.println(e.getMessage());
                return;
            }
        }

        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (BucketAlreadyOwnedByYouException e) {
            // Already owned by this account.
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public S3Client getS3() {
        return s3;
    }


    
    // EC2
    public String createEC2(String script, String tagName, int numberOfInstances) {
        return createEC2(script, tagName, numberOfInstances, UNKNOWN_FREE);
    }

    /**
     * Starts instances and tags them with {@code Name} and, when {@code freeSlots} is not
     * {@link #UNKNOWN_FREE}, with the manager free-slot hint. A caller starting a manager
     * passes {@code manager-clients-n} so the very first describe call already sees it.
     */
    public String createEC2(String script, String tagName, int numberOfInstances, int freeSlots) {
        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.builder().key("Name").value(tagName).build());
        if (freeSlots != UNKNOWN_FREE) {
            tags.add(Tag.builder().key(FREE_TAG).value(formatFreeSlots(freeSlots)).build());
        }
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(tags)
                        .build())
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        if (client.App.debug) {
            System.out.printf("[DEBUG] Successfully started EC2 instance %s based on AMI %s%n",
                    instanceId, ami);
        }
        return instanceId;
    }

    // ---- manager free-slot tag -------------------------------------------------

    /** EC2 tag key carrying how many more clients a manager can admit. */
    public static final String FREE_TAG = "free";

    /** Sentinel for "no usable free count" (missing or malformed tag). */
    public static final int UNKNOWN_FREE = -1;

    /** One entry of {@link #freeManagers()}: instance id plus its free-slot hint. */
    public static final class ManagerSlot extends FreeSlotTags.ManagerSlot {
        public ManagerSlot(String id, int free) {
            super(id, free);
        }
    }

    /** Pure compatibility delegates; they do not initialize the AWS singleton. */
    public static String formatFreeSlots(int free) {
        return FreeSlotTags.formatFreeSlots(free);
    }

    public static int parseFreeSlots(String raw) {
        return FreeSlotTags.parseFreeSlots(raw);
    }

    public static int freeSlotsOf(Map<String, String> tags) {
        return FreeSlotTags.freeSlotsOf(tags);
    }

    /**
     * Writes the manager's current free-slot count. Tags are a hint without
     * compare-and-set, so this is best effort and never fails the caller.
     */
    public void setFreeSlots(String managerID, int free) {
        if (managerID == null || managerID.isEmpty()) {
            return;
        }
        try {
            ec2.createTags(CreateTagsRequest.builder()
                    .resources(managerID)
                    .tags(Tag.builder().key(FREE_TAG).value(formatFreeSlots(free)).build())
                    .build());
            if (client.App.debug) {
                System.out.printf("[DEBUG] manager %s tagged %s=%s%n",
                        managerID, FREE_TAG, formatFreeSlots(free));
            }
        } catch (RuntimeException e) {
            System.err.println("[WARN] could not tag manager " + managerID + ": " + e.getMessage());
        }
    }

    /**
     * Number of live workers (running or pending). "Live" is stricter than "running" so a
     * worker that is still booting counts against the cap too.
     */
    public int runningWorkerCount() {
        return countLiveInstances("worker");
    }

    /** Number of live manager and worker instances, the manager included. */
    public int runningInstanceCount() {
        return countLiveInstances("manager") + countLiveInstances("worker");
    }

    private int countLiveInstances(String tagName) {
        int count = 0;
        String nextToken = null;
        try {
            while (true) {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(
                                Filter.builder().name("tag:Name").values(tagName).build(),
                                Filter.builder().name("instance-state-name")
                                        .values("running", "pending").build())
                        .nextToken(nextToken)
                        .build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    count += reservation.instances().size();
                }
                nextToken = response.nextToken();
                if (nextToken == null) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("Could not count live " + tagName + " instances", e);
        }
        return count;
    }

    /** Lists running managers with a usable {@code free} tag, most free first. */
    public List<ManagerSlot> freeManagers() {
        List<ManagerSlot> managers = new ArrayList<>();
        String nextToken = null;
        try {
            while (true) {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(
                                Filter.builder().name("tag:Name").values("manager").build(),
                                Filter.builder().name("tag-key").values(FREE_TAG).build(),
                                Filter.builder().name("instance-state-name").values("running", "pending").build())
                        .nextToken(nextToken)
                        .build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        Map<String, String> tags = new HashMap<>();
                        for (Tag tag : instance.tags()) {
                            tags.put(tag.key(), tag.value());
                        }
                        int free = freeSlotsOf(tags);
                        if (free != UNKNOWN_FREE) {
                            managers.add(new ManagerSlot(instance.instanceId(), free));
                        }
                    }
                }
                nextToken = response.nextToken();
                if (nextToken == null) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("Could not list running managers", e);
        }
        Collections.sort(managers, new Comparator<ManagerSlot>() {
            @Override
            public int compare(ManagerSlot a, ManagerSlot b) {
                return Integer.compare(b.free, a.free);
            }
        });
        return managers;
    }

    // SQS
    public SqsClient getSqs() {
        return sqs;
    }

    /**
     * Creates the queue if it does not exist yet (SQS createQueue is idempotent) and
     * returns its URL.
     */
    public String createQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.createQueue(createQueueRequest).queueUrl();
    }

    /** Deletes a queue, ignoring failures (the queue may already be gone). */
    public void deleteQueue(String queueUrl) {
        try {
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        } catch (Exception e) {
            // Best effort: nothing useful to do if the queue is already gone or unreachable.
        }
    }
}
