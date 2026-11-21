package utils;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    // Public Ubuntu 22.04 AMI (us-east-1); boot installs the JRE/AWS CLI if absent.
    public static String ami = "ami-0e72706534bb166e3";

    public static Region region1 = Region.US_EAST_1;
    public static Region region2 = Region.US_EAST_1;

    // Delay between DescribeInstances probes while instances settle.
    private static final long TERMINATE_POLL_INTERVAL_MILLIS = 2_000L;

    private AWS() {
        s3 = S3Client.builder().region(region1)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(10))
                        .socketTimeout(Duration.ofSeconds(30)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofSeconds(60))
                        .build())
                .build();
        // ReceiveMessage may long-poll for up to 20 seconds; keep both transport and
        // whole-call timeouts comfortably above that wait.
        sqs = SqsClient.builder().region(region1)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(10))
                        .socketTimeout(Duration.ofSeconds(30)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofSeconds(45))
                        .build())
                .build();
        ec2 = Ec2Client.builder().region(region2)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(10))
                        .socketTimeout(Duration.ofSeconds(30)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofSeconds(60))
                        .build())
                .build();
        this.bucketName = resolveBucketName();
    }

    // Number of live manager instances (running or pending).
    public int liveManagerCount() {
        return countLiveInstances("manager");
    }

    // Number of instances tagged tagName in any of the given EC2 states. A shutting-down worker
    // still occupies the fleet (the cap / its slot) until EC2 finishes removing it, so callers
    // counting workers include "shutting-down" among the live states.
    private int countInstances(String tagName, String... states) {
        if (states == null || states.length == 0) {
            throw new IllegalArgumentException("at least one instance state is required");
        }
        int count = 0;
        String nextToken = null;
        try {
            while (true) {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(
                                Filter.builder().name("tag:Name").values(tagName).build(),
                                Filter.builder().name("instance-state-name").values(states).build())
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
            throw new IllegalStateException("Could not count " + tagName + " instances in "
                    + String.join("/", states), e);
        }
        return count;
    }

    public static AWS getInstance() {
        return InstanceHolder.INSTANCE;
    }

    private static final class InstanceHolder {
        private static final AWS INSTANCE = new AWS();
    }
public String bucketName;

  private String resolveBucketName() {
      String envBucket = System.getenv("S3_BUCKET");
      if (envBucket == null || envBucket.isBlank()) {
          envBucket = System.getenv("AWS_BUCKET_NAME");
      }
      if (envBucket != null && !envBucket.isBlank()) {
          return envBucket.trim();
      }
      if (s3 != null) {
          try {
              for (Bucket b : s3.listBuckets().buckets()) {
                  if (b.name() != null && b.name().startsWith("dsp1-only-together2-")) {
                      return b.name();
                  }
              }
          } catch (RuntimeException ignored) {
              // Fall back cleanly when offline or without credentials.
          }
      }
      return "dsp1-only-together2";
  }
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

    // Starts numberOfInstances instances tagged with Name and returns every launched instance
    // ID. A count below one launches nothing.
    public List<String> createEC2Instances(String script, String tagName, int numberOfInstances) {
        return createEC2Instances(script, tagName, numberOfInstances, UNKNOWN_FREE);
    }

    private List<String> createEC2Instances(String script, String tagName, int numberOfInstances,
                                            int freeSlots) {
        if (numberOfInstances < 1) {
            return Collections.emptyList();
        }
        List<Tag> tags = buildInstanceTags(tagName, freeSlots);
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(numberOfInstances)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(tags)
                        .build())
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        List<String> instanceIds = new ArrayList<String>();
        for (Instance instance : response.instances()) {
            instanceIds.add(instance.instanceId());
        }
        if (client.App.debug) {
            System.out.printf("[DEBUG] Successfully started %d EC2 instance(s) based on AMI %s: %s%n",
                    instanceIds.size(), ami, instanceIds);
        }
        return instanceIds;
    }

    public String createEC2(String script, String tagName, int numberOfInstances) {
        return firstInstanceId(createEC2Instances(script, tagName, numberOfInstances));
    }

    // Starts instances and tags them with Name and, when freeSlots is not UNKNOWN_FREE, with the
    // manager free-slot hint. A caller starting a manager passes manager-clients-n so the very
    // first describe call already sees it.
    public String createEC2(String script, String tagName, int numberOfInstances, int freeSlots) {
        return firstInstanceId(createEC2Instances(script, tagName, numberOfInstances, freeSlots));
    }

    private static String firstInstanceId(List<String> instanceIds) {
        return instanceIds.isEmpty() ? null : instanceIds.get(0);
    }

    // ---- instance tag building --------------------------------------------------

    // Pure tag set for a new instance: always Name, plus the free-slot hint when freeSlots is
    // not UNKNOWN_FREE. No AWS client is touched, so tests can call it directly.
    public static List<Tag> buildInstanceTags(String tagName, int freeSlots) {
        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.builder().key("Name").value(tagName).build());
        if (freeSlots != UNKNOWN_FREE) {
            tags.add(Tag.builder().key(FREE_TAG).value(formatFreeSlots(freeSlots)).build());
        }
        return tags;
    }

    // ---- manager free-slot tag -------------------------------------------------

    // EC2 tag key carrying how many more clients a manager can admit.
    public static final String FREE_TAG = "free";

    // Sentinel for "no usable free count" (missing or malformed tag).
    public static final int UNKNOWN_FREE = -1;

    // One entry of freeManagers(): instance id plus its free-slot hint.
    public static final class ManagerSlot extends FreeSlotTags.ManagerSlot {
        public ManagerSlot(String id, int free) {
            super(id, free);
        }
    }

    // Pure compatibility delegates; they do not initialize the AWS singleton.
    public static String formatFreeSlots(int free) {
        return FreeSlotTags.formatFreeSlots(free);
    }

    public static int parseFreeSlots(String raw) {
        return FreeSlotTags.parseFreeSlots(raw);
    }

    public static int freeSlotsOf(Map<String, String> tags) {
        return FreeSlotTags.freeSlotsOf(tags);
    }

    // Writes the manager's current free-slot count. Tags are a hint without compare-and-set, so
    // this is best effort and never fails the caller.
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

    // Number of live workers: running, pending, or shutting-down. "Live" is stricter than
    // "running" so a worker that is still booting counts against the cap too, and a
    // shutting-down worker still occupies the fleet until EC2 removes it, so counting it as
    // live avoids launching a replacement while it terminates.
    public int runningWorkerCount() {
        return countInstances("worker", "running", "pending", "shutting-down");
    }

    // Number of live manager and worker instances, the manager included. Managers count as
    // running + pending only; workers additionally count while shutting-down.
    public int runningInstanceCount() {
        return countLiveInstances("manager") + countInstances("worker", "running", "pending", "shutting-down");
    }

    // Live instances of a role in the strict states: running or pending (managers).
    private int countLiveInstances(String tagName) {
        return countInstances(tagName, "running", "pending");
    }

    // Current state per instance ID; an ID EC2 no longer reports is simply absent. A filter
    // (not instanceIds) keeps an unknown ID from failing the whole probe.
    public Map<String, String> instanceStates(Collection<String> instanceIds) {
        Map<String, String> states = new HashMap<>();
        if (instanceIds == null || instanceIds.isEmpty()) {
            return states;
        }
        List<String> ids = new ArrayList<String>(instanceIds);
        String nextToken = null;
        while (true) {
            DescribeInstancesResponse response = ec2.describeInstances(DescribeInstancesRequest.builder()
                    .filters(Filter.builder().name("instance-id").values(ids).build())
                    .nextToken(nextToken)
                    .build());
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    String state = instance.state() == null ? null : instance.state().nameAsString();
                    states.put(instance.instanceId(), state);
                }
            }
            nextToken = response.nextToken();
            if (nextToken == null) {
                break;
            }
        }
        return states;
    }

    // Terminates every given instance and waits until each ID is settled: terminated,
    // shutting-down, stopped, or no longer reported by EC2. There is no timeout; a worker that
    // never settles keeps the manager waiting. AWS errors are logged and swallowed so
    // termination is never blocked by a failed call.
    public void terminateInstances(List<String> instanceIds) {
        if (instanceIds == null || instanceIds.isEmpty()) {
            return;
        }
        List<String> ids = new ArrayList<String>(instanceIds);
        try {
            ec2.terminateInstances(TerminateInstancesRequest.builder()
                    .instanceIds(ids)
                    .build());
            if (client.App.debug) {
                System.out.printf("[DEBUG] requested termination of %d instance(s): %s%n",
                        ids.size(), ids);
            }
        } catch (RuntimeException e) {
            System.err.println("[AWS] could not terminate instances " + ids + ": " + e.getMessage());
            return;
        }

        while (true) {
            Map<String, String> states;
            try {
                states = instanceStates(ids);
            } catch (RuntimeException e) {
                System.err.println("[AWS] could not read instance states while stopping " + ids
                        + ": " + e.getMessage());
                return;
            }
            if (allSettled(ids, states)) {
                return;
            }
            try {
                Thread.sleep(TERMINATE_POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    // True when every ID is terminated, shutting down, stopped, or gone from EC2.
    private static boolean allSettled(List<String> instanceIds, Map<String, String> states) {
        for (String id : instanceIds) {
            String state = states.get(id);
            if (state == null) {
                continue; // no longer reported: the instance is gone
            }
            if (!"terminated".equals(state)
                    && !"shutting-down".equals(state)
                    && !"stopped".equals(state)) {
                return false;
            }
        }
        return true;
    }

    // Lists running managers with a usable free tag, most free first.
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

    // Creates the queue if it does not exist yet (createQueue is idempotent) and returns its URL.
    public String createQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.createQueue(createQueueRequest).queueUrl();
    }

    // Deletes a queue, ignoring failures (the queue may already be gone).
    public void deleteQueue(String queueUrl) {
        try {
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        } catch (Exception e) {
            // Best effort: nothing useful to do if the queue is already gone or unreachable.
        }
    }
}
