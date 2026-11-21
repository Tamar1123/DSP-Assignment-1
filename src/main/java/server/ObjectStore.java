package server;

import utils.AWS;
import utils.Queues;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Reads input URL lists and writes summary files; local files in dev mode, S3 otherwise.
class ObjectStore {
    void writeSummary(List<String> urls, Map<String, String> texts, String summaryLocation) throws IOException {
        // In dev mode summaryLocation is a local path; on AWS it is an S3 key, so go via a temp file.
        File file = Queues.isDev() ? new File(summaryLocation) : File.createTempFile("manager-summary-", ".txt");
        PrintWriter out = new PrintWriter(file);
        try {
            // Input URL order (urls), never the map's order.
            for (String url : urls) {
                String text = texts.get(url);
                text = (text == null ? "" : text).replace('\n', ' ').replace('\r', ' ');
                out.println(url + "\t" + text);
            }
        } finally {
            out.close();
        }
        if (!Queues.isDev()) {
            String s3Key = summaryLocation;
            AWS.getInstance().getS3().putObject(
                    software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                            .bucket(AWS.getInstance().bucketName)
                            .key(s3Key)
                            .build(),
                    software.amazon.awssdk.core.sync.RequestBody.fromFile(file));
            file.delete();
        }
    }

    List<String> readUrls(String inputFileLocation) throws IOException {
        if (!Queues.isDev()) {
            // Download from S3 to a temp file, then parse.
            File tempFile = File.createTempFile("manager-input-", ".txt");
            tempFile.deleteOnExit();
            tempFile.delete(); // the SDK refuses to overwrite an existing file
            AWS.getInstance().getS3().getObject(
                    software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                            .bucket(AWS.getInstance().bucketName)
                            .key(inputFileLocation)
                            .build(),
                    software.amazon.awssdk.core.sync.ResponseTransformer.toFile(tempFile));
            return readUrlsFromFile(tempFile);
        }
        return readUrlsFromFile(new File(inputFileLocation));
    }

    private List<String> readUrlsFromFile(File file) throws IOException {
        List<String> urls = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                String url = line.trim();
                if (!url.isEmpty()) {
                    urls.add(url);
                }
            }
        } finally {
            reader.close();
        }
        return urls;
    }
}
