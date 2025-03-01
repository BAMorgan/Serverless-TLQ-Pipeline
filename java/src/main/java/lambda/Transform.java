package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Transform implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private static final String TRANSFORMED_CSV_BUCKET_NAME = "tcss462-term-project";
    private static final Logger logger = Logger.getLogger(Transform.class.getName());
    private static final int BUFFER_SIZE = 8192;
    private static final Map<String, String> PRIORITY_MAPPING = Map.of(
            "L", "Low",
            "M", "Medium",
            "H", "High",
            "C", "Critical"
    );
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy");

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        String localInputPath = null;
        String localOutputPath = null;

        try {
            // Extract and validate body
            Map<String, String> body = extractBody(event);
            String bucketName = body.get("bucket_name");
            String fileKey = body.get("key");

            inspector.addAttribute("bucket_name", bucketName);
            inspector.addAttribute("file_key", fileKey);

            // Setup file paths
            localInputPath = "/tmp/" + fileKey;
            localOutputPath = "/tmp/transformed_" + fileKey;

            // Download and process file
            downloadFromS3(bucketName, fileKey, localInputPath);
            transformFile(localInputPath, localOutputPath);

            // Upload transformed file
            String transformedKey = "transformed_" + fileKey;
            uploadToS3(localOutputPath, transformedKey);

            // Add transformation attributes and prepare response
            inspector.addAttribute("transformed_bucket", TRANSFORMED_CSV_BUCKET_NAME);
            inspector.addAttribute("transformed_key", transformedKey);

            Response response = new Response();
            response.setValue("File transformed and uploaded successfully");
            inspector.consumeResponse(response);

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing the event", e);
            Response response = new Response();
            inspector.consumeResponse(response);
        } finally {
            // Cleanup temporary files
            cleanupFiles(localInputPath, localOutputPath);
            inspector.inspectAllDeltas();
        }

        return inspector.finish();
    }

    private Map<String, String> extractBody(Map<String, Object> event) throws IOException {
        Object bodyObj = event.get("body");
        if (bodyObj == null) {
            throw new IllegalArgumentException("The 'body' key is missing from the event payload");
        }

        if (bodyObj instanceof String) {
            return new ObjectMapper().readValue((String) bodyObj, HashMap.class);
        } else if (bodyObj instanceof Map) {
            return (Map<String, String>) bodyObj;
        }
        throw new IllegalArgumentException("Unsupported body type in event");
    }

    private void downloadFromS3(String bucketName, String fileKey, String localPath) throws IOException {
        try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileKey));
             BufferedInputStream bis = new BufferedInputStream(s3Object.getObjectContent(), BUFFER_SIZE);
             BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(localPath), BUFFER_SIZE)) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = bis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
    }

    private void transformFile(String inputPath, String outputPath) throws IOException {
        Set<String> seenOrderIds = new HashSet<>();
        List<String> headers = null;

        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath), BUFFER_SIZE);
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath), BUFFER_SIZE)) {

            // Process header
            String headerLine = reader.readLine();
            if (headerLine != null) {
                headers = new ArrayList<>(Arrays.asList(headerLine.split(",")));
                headers.add("Order Processing Time");
                headers.add("Gross Margin");
                writer.write(String.join(",", headers));
                writer.newLine();
            }

            // Process data rows
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length < headers.size() - 2) {
                    continue;
                }

                String orderId = values[getColumnIndex(headers, "Order ID")];
                if (seenOrderIds.contains(orderId)) {
                    continue;
                }
                seenOrderIds.add(orderId);

                // Transform the row
                String transformedLine = transformRow(values, headers);
                if (transformedLine != null) {
                    writer.write(transformedLine);
                    writer.newLine();
                }
            }
        }
    }

    private String transformRow(String[] values, List<String> headers) {
        try {
            StringBuilder sb = new StringBuilder();

            // Copy original values
            for (int i = 0; i < values.length; i++) {
                if (i > 0) sb.append(",");

                // Transform Order Priority if needed
                if (headers.get(i).equals("Order Priority")) {
                    sb.append(PRIORITY_MAPPING.getOrDefault(values[i], "Unknown"));
                } else {
                    sb.append(values[i]);
                }
            }

            // Add Order Processing Time
            sb.append(",");
            try {
                Date orderDate = DATE_FORMAT.parse(values[getColumnIndex(headers, "Order Date")]);
                Date shipDate = DATE_FORMAT.parse(values[getColumnIndex(headers, "Ship Date")]);
                sb.append((shipDate.getTime() - orderDate.getTime()) / (1000 * 60 * 60 * 24));
            } catch (ParseException e) {
                sb.append("0");
            }

            // Add Gross Margin
            sb.append(",");
            try {
                double totalProfit = Double.parseDouble(values[getColumnIndex(headers, "Total Profit")]);
                double totalRevenue = Double.parseDouble(values[getColumnIndex(headers, "Total Revenue")]);
                sb.append(totalRevenue != 0 ? totalProfit / totalRevenue : "0");
            } catch (NumberFormatException e) {
                sb.append("0");
            }

            return sb.toString();
        } catch (Exception e) {
            logger.warning("Error transforming row: " + Arrays.toString(values));
            return null;
        }
    }

    private void uploadToS3(String localPath, String s3Key) throws IOException {
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(localPath), BUFFER_SIZE)) {
            s3Client.putObject(new PutObjectRequest(TRANSFORMED_CSV_BUCKET_NAME, s3Key, bis, null));
        }
    }

    private int getColumnIndex(List<String> headers, String columnName) {
        return headers.indexOf(columnName);
    }

    private void cleanupFiles(String... paths) {
        for (String path : paths) {
            if (path != null) {
                try {
                    new File(path).delete();
                } catch (Exception e) {
                    logger.warning("Failed to delete temporary file: " + path);
                }
            }
        }
    }
}