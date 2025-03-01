package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Query implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private static final Logger logger = Logger.getLogger(Query.class.getName());
    private static final String LOCAL_DB_PATH = "/tmp/data.db";
    private static final int BUFFER_SIZE = 8192;

    static {
        try {
            // Explicitly load the SQLite JDBC driver
            Class.forName("org.sqlite.JDBC");
            logger.info("SQLite JDBC driver loaded successfully");
        } catch (ClassNotFoundException e) {
            logger.severe("Failed to load SQLite JDBC driver: " + e.getMessage());
            throw new RuntimeException("Failed to load SQLite JDBC driver", e);
        }
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        logger.info("Event received: " + event);
        Response response = new Response();

        try {
            // Validate and extract parameters
            String bucketName = (String) event.get("bucket_name");
            String dbKey = (String) event.get("key");

            if (bucketName == null || dbKey == null) {
                throw new IllegalArgumentException("Both 'bucket_name' and 'key' are required.");
            }

            // Add request attributes to Inspector
            inspector.addAttribute("bucket_name", bucketName);
            inspector.addAttribute("db_key", dbKey);

            // Download and process database
            downloadDatabase(bucketName, dbKey);

            // Process queries and get results
            Map<String, Object> results = processQueries(event);

            // Set successful response with results
            response.setValue(results.toString());
            inspector.addAttribute("query_results", results);

        } catch (Exception e) {
            logger.log(Level.SEVERE, "An error occurred", e);
            response.setValue(String.valueOf(new HashMap<String, Object>() {{
                put("error", e.getMessage());
                put("error_type", e.getClass().getSimpleName());
            }}));
        } finally {
            cleanup();
            inspector.consumeResponse(response);
            inspector.inspectAllDeltas();
        }

        return inspector.finish();
    }

    private void downloadDatabase(String bucketName, String dbKey) throws IOException {
        try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, dbKey));
             BufferedInputStream bis = new BufferedInputStream(s3Object.getObjectContent(), BUFFER_SIZE);
             BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(LOCAL_DB_PATH), BUFFER_SIZE)) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = bis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
    }

    private Map<String, Object> processQueries(Map<String, Object> event) throws SQLException {
        Map<String, Object> results = new HashMap<>();

        try (Connection conn = getOptimizedConnection()) {
            @SuppressWarnings("unchecked")
            Map<String, String> filters = (Map<String, String>) event.getOrDefault("Filters", new HashMap<>());
            @SuppressWarnings("unchecked")
            List<String> groupBy = (List<String>) event.getOrDefault("Group By", new ArrayList<>());

            String sql = buildAggregationQuery(filters, groupBy);
            logger.info("Executing query: " + sql);

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                setQueryParameters(stmt, filters);

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        results.putAll(extractAggregations(rs, groupBy));
                    }
                }
            }
        }

        return results;
    }

    private void setQueryParameters(PreparedStatement stmt, Map<String, String> filters) throws SQLException {
        int paramIndex = 1;
        for (String value : filters.values()) {
            stmt.setString(paramIndex++, value);
        }
    }

    private Connection getOptimizedConnection() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + LOCAL_DB_PATH);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode = WAL");
            stmt.execute("PRAGMA synchronous = NORMAL");
            stmt.execute("PRAGMA cache_size = -2000");
            stmt.execute("PRAGMA temp_store = MEMORY");
        }
        return conn;
    }

    private String buildAggregationQuery(Map<String, String> filters, List<String> groupBy) {
        StringBuilder sql = new StringBuilder();

        // Select clause with proper type handling
        sql.append("""
            SELECT 
                CAST(ROUND(AVG(CAST(OrderProcessingTime AS REAL)), 2) AS TEXT) AS AvgOrderProcessingTime,
                CAST(ROUND(AVG(CAST(GrossMargin AS REAL)), 4) AS TEXT) AS AvgGrossMargin,
                CAST(ROUND(AVG(CAST(UnitsSold AS REAL)), 2) AS TEXT) AS AvgUnitsSold,
                CAST(MAX(UnitsSold) AS TEXT) AS MaxUnitsSold,
                CAST(MIN(UnitsSold) AS TEXT) AS MinUnitsSold,
                CAST(SUM(UnitsSold) AS TEXT) AS TotalUnitsSold,
                CAST(ROUND(SUM(TotalRevenue), 2) AS TEXT) AS TotalRevenue,
                CAST(ROUND(SUM(TotalProfit), 2) AS TEXT) AS TotalProfit,
                CAST(COUNT(DISTINCT OrderID) AS TEXT) AS NumberOfOrders
        """);

        // Add grouping columns if present
        if (!groupBy.isEmpty()) {
            sql.append(", ").append(String.join(", ", groupBy));
        }

        sql.append(" FROM orders");

        // Add WHERE clause if filters exist
        if (!filters.isEmpty()) {
            sql.append(" WHERE ")
                    .append(String.join(" AND ",
                            filters.keySet().stream()
                                    .map(key -> sanitizeColumnName(key) + " = ?")
                                    .toList()
                    ));
        }

        // Add GROUP BY clause if grouping fields exist
        if (!groupBy.isEmpty()) {
            sql.append(" GROUP BY ")
                    .append(String.join(", ",
                            groupBy.stream()
                                    .map(this::sanitizeColumnName)
                                    .toList()
                    ));
        }

        return sql.toString();
    }

    private Map<String, Object> extractAggregations(ResultSet rs, List<String> groupBy) throws SQLException {
        Map<String, Object> row = new HashMap<>();

        // Extract metrics
        row.put("Average Order Processing Time in days", rs.getString("AvgOrderProcessingTime"));
        row.put("Average Gross Margin", rs.getString("AvgGrossMargin"));
        row.put("Average Units Sold", rs.getString("AvgUnitsSold"));
        row.put("Max Units Sold", rs.getString("MaxUnitsSold"));
        row.put("Min Units Sold", rs.getString("MinUnitsSold"));
        row.put("Total Units Sold", rs.getString("TotalUnitsSold"));
        row.put("Total Revenue", rs.getString("TotalRevenue"));
        row.put("Total Profit", rs.getString("TotalProfit"));
        row.put("Number of Orders", rs.getString("NumberOfOrders"));

        // Extract grouping columns if present
        for (String column : groupBy) {
            row.put(column, rs.getString(column));
        }

        return row;
    }

    private String sanitizeColumnName(String columnName) {
        return columnName.replaceAll("[^a-zA-Z0-9_]", "");
    }

    private void cleanup() {
        try {
            new File(LOCAL_DB_PATH).delete();
        } catch (Exception e) {
            logger.warning("Failed to delete temporary database file: " + e.getMessage());
        }
    }
}