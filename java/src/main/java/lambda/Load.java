package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Load implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private static final Logger logger = Logger.getLogger(Load.class.getName());
    private static final int BATCH_SIZE = 1000;
    private static final int BUFFER_SIZE = 8192;

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        logger.info("Event received: " + event);

        try {
            // Extract parameters
            String bucketName = (String) event.get("bucket_name");
            String csvFileKey = (String) event.get("csv_file_key");
            String dbFileName = (String) event.getOrDefault("db_file_name", "data.db");

            if (bucketName == null || csvFileKey == null) {
                throw new IllegalArgumentException("Both 'bucket_name' and 'csv_file_key' are required.");
            }

            inspector.addAttribute("bucket_name", bucketName);
            inspector.addAttribute("csv_file_key", csvFileKey);
            inspector.addAttribute("db_file_name", dbFileName);

            String localCsvPath = "/tmp/" + Paths.get(csvFileKey).getFileName();
            String localDbPath = "/tmp/" + dbFileName;

            // Download CSV using buffered streaming
            downloadCsvFromS3(bucketName, csvFileKey, localCsvPath);

            // Process CSV and create database
            processDataAndCreateDb(localCsvPath, localDbPath);

            // Upload database to S3
            String s3Key = "databases/" + dbFileName;
            uploadDatabaseToS3(localDbPath, bucketName, s3Key);

            // Prepare response
            Response response = new Response();
            response.setValue("Database created and uploaded successfully");
            inspector.addAttribute("s3_upload_key", s3Key);
            inspector.consumeResponse(response);

        } catch (Exception e) {
            logger.log(Level.SEVERE, "An error occurred", e);
            Response response = new Response();
            inspector.consumeResponse(response);
        } finally {
            // Cleanup temporary files
            //cleanupTempFiles(localCsvPath, localDbPath);
        }

        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private void downloadCsvFromS3(String bucketName, String csvFileKey, String localPath) throws IOException {
        logger.info("Downloading CSV file from S3: " + bucketName + "/" + csvFileKey);

        try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, csvFileKey));
             BufferedInputStream bis = new BufferedInputStream(s3Object.getObjectContent(), BUFFER_SIZE);
             BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(localPath), BUFFER_SIZE)) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = bis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
        logger.info("CSV file downloaded successfully");
    }

    private void processDataAndCreateDb(String csvPath, String dbPath) throws Exception {
        Class.forName("org.sqlite.JDBC");

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.setAutoCommit(false);  // Enable transaction mode

            // Create table with optimized indices
            createTableWithIndices(conn);

            // Process CSV in batches
            try (BufferedReader br = new BufferedReader(new FileReader(csvPath), BUFFER_SIZE)) {
                br.readLine(); // Skip header
                processDataInBatches(br, conn);
            }
        }
    }

    private void createTableWithIndices(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            // Create table with appropriate data types and constraints
            stmt.execute("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Region TEXT NOT NULL,
                    Country TEXT NOT NULL,
                    ItemType TEXT NOT NULL,
                    SalesChannel TEXT NOT NULL,
                    OrderPriority TEXT NOT NULL,
                    OrderDate TEXT NOT NULL,
                    OrderID INTEGER NOT NULL,
                    ShipDate TEXT NOT NULL,
                    UnitsSold INTEGER NOT NULL,
                    UnitPrice REAL NOT NULL,
                    UnitCost REAL NOT NULL,
                    TotalRevenue REAL NOT NULL,
                    TotalCost REAL NOT NULL,
                    TotalProfit REAL NOT NULL,
                    OrderProcessingTime INTEGER NOT NULL,
                    GrossMargin REAL NOT NULL
                );
                
                CREATE INDEX idx_orderid ON orders(OrderID);
                CREATE INDEX idx_region_country ON orders(Region, Country);
                CREATE INDEX idx_orderdate ON orders(OrderDate);
            """);
            conn.commit();
        }
    }

    private void processDataInBatches(BufferedReader br, Connection conn) throws Exception {
        String insertQuery = """
            INSERT INTO orders (
                Region, Country, ItemType, SalesChannel, OrderPriority,
                OrderDate, OrderID, ShipDate, UnitsSold, UnitPrice, UnitCost,
                TotalRevenue, TotalCost, TotalProfit, OrderProcessingTime, GrossMargin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """;

        try (PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {
            String line;
            int count = 0;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                bindParameters(pstmt, values);
                pstmt.addBatch();
                count++;

                if (count % BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    pstmt.clearBatch();
                }
            }

            // Execute remaining batch
            if (count % BATCH_SIZE != 0) {
                pstmt.executeBatch();
                conn.commit();
            }
        }
    }

    private void bindParameters(PreparedStatement pstmt, String[] values) throws Exception {
        pstmt.setString(1, values[0]);  // Region
        pstmt.setString(2, values[1]);  // Country
        pstmt.setString(3, values[2]);  // ItemType
        pstmt.setString(4, values[3]);  // SalesChannel
        pstmt.setString(5, values[4]);  // OrderPriority
        pstmt.setString(6, values[5]);  // OrderDate
        pstmt.setInt(7, Integer.parseInt(values[6]));  // OrderID
        pstmt.setString(8, values[7]);  // ShipDate
        pstmt.setInt(9, Integer.parseInt(values[8]));  // UnitsSold
        pstmt.setDouble(10, Double.parseDouble(values[9]));  // UnitPrice
        pstmt.setDouble(11, Double.parseDouble(values[10])); // UnitCost
        pstmt.setDouble(12, Double.parseDouble(values[11])); // TotalRevenue
        pstmt.setDouble(13, Double.parseDouble(values[12])); // TotalCost
        pstmt.setDouble(14, Double.parseDouble(values[13])); // TotalProfit
        pstmt.setInt(15, Integer.parseInt(values[14]));      // OrderProcessingTime
        pstmt.setDouble(16, Double.parseDouble(values[15])); // GrossMargin
    }

    private void uploadDatabaseToS3(String dbPath, String bucketName, String s3Key) throws IOException {
        logger.info("Uploading SQLite database to S3: " + bucketName + "/" + s3Key);

        try (BufferedInputStream dbFileStream = new BufferedInputStream(new FileInputStream(dbPath), BUFFER_SIZE)) {
            s3Client.putObject(new PutObjectRequest(bucketName, s3Key, dbFileStream, null));
        }
        logger.info("Database uploaded successfully");
    }

    private void cleanupTempFiles(String... filePaths) {
        for (String path : filePaths) {
            try {
                new File(path).delete();
            } catch (Exception e) {
                logger.warning("Failed to delete temporary file: " + path);
            }
        }
    }
}