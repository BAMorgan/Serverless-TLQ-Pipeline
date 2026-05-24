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
    private static final Map<String, String> FILTER_COLUMNS = Map.of(
            "Region", "Region",
            "Country", "Country",
            "Item Type", "ItemType",
            "Sales Channel", "SalesChannel",
            "Order Priority", "OrderPriority"
    );

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
            Map<String, Object> results = processQueries(event, LOCAL_DB_PATH);

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

    Map<String, Object> processQueries(Map<String, Object> event, String dbPath) throws SQLException {
        Map<String, Object> response = new LinkedHashMap<>();
        List<Map<String, Object>> results = new ArrayList<>();
        boolean includeResults = false;

        try (Connection conn = getOptimizedConnection(dbPath)) {
            @SuppressWarnings("unchecked")
            Map<String, String> filters = (Map<String, String>) event.getOrDefault("Filters", new HashMap<>());
            @SuppressWarnings("unchecked")
            List<String> groupBy = (List<String>) event.getOrDefault("Group By", new ArrayList<>());

            QueryPlan plan = buildAggregationQuery(filters, groupBy);
            includeResults = !plan.groupColumns().isEmpty();
            logger.info("Executing query: " + plan.sql());

            try (PreparedStatement stmt = conn.prepareStatement(plan.sql())) {
                setQueryParameters(stmt, plan.params());

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        results.add(extractAggregations(rs, plan.groupColumns()));
                    }
                }
            }
        }

        response.put("statusCode", 200);
        response.put("aggregations", results.isEmpty() ? new LinkedHashMap<>() : results.get(0));
        if (includeResults) {
            response.put("results", results);
        }
        return response;
    }

    private void setQueryParameters(PreparedStatement stmt, List<String> params) throws SQLException {
        int paramIndex = 1;
        for (String value : params) {
            stmt.setString(paramIndex++, value);
        }
    }

    private Connection getOptimizedConnection(String dbPath) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode = WAL");
            stmt.execute("PRAGMA synchronous = NORMAL");
            stmt.execute("PRAGMA cache_size = -2000");
            stmt.execute("PRAGMA temp_store = MEMORY");
        }
        return conn;
    }

    record QueryPlan(String sql, List<String> params, List<String> groupColumns) {}

    static QueryPlan buildAggregationQuery(Map<String, String> filters, List<String> groupBy) {
        StringBuilder sql = new StringBuilder();
        List<String> params = new ArrayList<>();
        List<String> groupColumns = dedupeGroupColumns(groupBy);

        // Select clause with proper type handling
        sql.append("""
            SELECT 
                ROUND(AVG(CAST(OrderProcessingTime AS REAL)), 2) AS AvgOrderProcessingTime,
                ROUND(AVG(CAST(GrossMargin AS REAL)), 4) AS AvgGrossMargin,
                ROUND(AVG(CAST(UnitsSold AS REAL)), 2) AS AvgUnitsSold,
                MAX(UnitsSold) AS MaxUnitsSold,
                MIN(UnitsSold) AS MinUnitsSold,
                SUM(UnitsSold) AS TotalUnitsSold,
                ROUND(SUM(TotalRevenue), 2) AS TotalRevenue,
                ROUND(SUM(TotalProfit), 2) AS TotalProfit,
                COUNT(DISTINCT OrderID) AS NumberOfOrders
        """);

        // Add grouping columns if present
        if (!groupColumns.isEmpty()) {
            sql.append(", ").append(String.join(", ", groupColumns));
        }

        sql.append(" FROM orders");

        // Add WHERE clause if filters exist
        if (!filters.isEmpty()) {
            List<String> whereClauses = new ArrayList<>();
            for (Map.Entry<String, String> entry : filters.entrySet()) {
                String mappedColumn = FILTER_COLUMNS.get(entry.getKey());
                if (mappedColumn == null) {
                    throw new IllegalArgumentException("Unsupported filter column: " + entry.getKey());
                }
                whereClauses.add(mappedColumn + " = ?");
                params.add(entry.getValue());
            }
            sql.append(" WHERE ").append(String.join(" AND ", whereClauses));
        }

        // Add GROUP BY clause if grouping fields exist
        if (!groupColumns.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupColumns));
            sql.append(" ORDER BY ").append(String.join(", ", groupColumns));
        }

        return new QueryPlan(sql.toString(), params, groupColumns);
    }

    private Map<String, Object> extractAggregations(ResultSet rs, List<String> groupBy) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();

        // Extract metrics
        row.put("Average Order Processing Time in days", rs.getDouble("AvgOrderProcessingTime"));
        row.put("Average Gross Margin", rs.getDouble("AvgGrossMargin"));
        row.put("Average Units Sold", rs.getDouble("AvgUnitsSold"));
        row.put("Max Units Sold", rs.getInt("MaxUnitsSold"));
        row.put("Min Units Sold", rs.getInt("MinUnitsSold"));
        row.put("Total Units Sold", rs.getInt("TotalUnitsSold"));
        row.put("Total Revenue", rs.getDouble("TotalRevenue"));
        row.put("Total Profit", rs.getDouble("TotalProfit"));
        row.put("Number of Orders", rs.getInt("NumberOfOrders"));

        // Extract grouping columns if present
        for (String column : groupBy) {
            row.put(column, rs.getString(column));
        }

        return row;
    }

    private static List<String> dedupeGroupColumns(List<String> groupBy) {
        Set<String> seen = new LinkedHashSet<>();
        for (String column : groupBy) {
            String mappedColumn = FILTER_COLUMNS.get(column);
            if (mappedColumn == null) {
                throw new IllegalArgumentException("Unsupported group by column: " + column);
            }
            seen.add(mappedColumn);
        }
        return new ArrayList<>(seen);
    }

    private void cleanup() {
        try {
            new File(LOCAL_DB_PATH).delete();
        } catch (Exception e) {
            logger.warning("Failed to delete temporary database file: " + e.getMessage());
        }
    }
}
