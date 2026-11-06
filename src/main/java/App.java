public class App {
    final static AWS aws = AWS.getInstance();
    static String inFilePath = "";
    static String outFilePath = "";
    static int tasksPerWorker = 0;
    static boolean debug = false;
    static boolean terminate = false;
    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional) -D (Debug, optional)]
        if (args.length < 3) {
            throw new IllegalArgumentException("Not enough arguments. Usage: java App <inFilePath> <outFilePath> <tasksPerWorker> [-t] [-D]");
        }
        inFilePath = args[0];
        outFilePath = args[1];
        tasksPerWorker = Integer.parseInt(args[2]);
        for (String arg : args) {
            if (arg.equals("-t")) {
                terminate = true;
            }
            if (arg.equals("-D")) {
                debug = true;
            }
        }
        try {
            setup();
            createEC2();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //Create Buckets, Create Queues, Upload JARs to S3
    private static void setup() {
        if (debug)
            System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists(aws.bucketName);
    }

    private static void createEC2() {
        String ec2Script = "#!/bin/bash\n" +
                "echo Hello World\n";
        String managerInstanceID = aws.createEC2(ec2Script, "thisIsJustAString", 1);
    }
}
