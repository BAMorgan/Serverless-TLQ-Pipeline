package lambda;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryTest {

    private static final Path FIXTURES = Path.of("..", "tests", "fixtures");

    @TempDir
    Path tempDir;

    @Test
    void buildAggregationQueryUsesMappedColumnsAndParameters() {
        Map<String, String> filters = new LinkedHashMap<>();
        filters.put("Sales Channel", "Online");

        Query.QueryPlan plan = Query.buildAggregationQuery(filters, List.of("Region", "Region"));

        assertTrue(plan.sql().contains("WHERE SalesChannel = ?"));
        assertTrue(plan.sql().contains("GROUP BY Region ORDER BY Region"));
        assertEquals(List.of("Online"), plan.params());
        assertEquals(List.of("Region"), plan.groupColumns());
    }

    @Test
    void buildAggregationQueryRejectsUnsupportedColumns() {
        Map<String, String> filters = new LinkedHashMap<>();
        filters.put("Total Revenue", "100");

        assertThrows(
                IllegalArgumentException.class,
                () -> Query.buildAggregationQuery(filters, List.of())
        );
    }

    @Test
    void processQueriesPreservesGroupedResultRows() throws Exception {
        Path dbPath = tempDir.resolve("orders.db");
        createOrdersDatabase(dbPath);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("Filters", Map.of("Sales Channel", "Online"));
        event.put("Group By", List.of("Region"));

        Map<String, Object> response = new Query().processQueries(event, dbPath.toString());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("results");

        assertEquals(2, results.size());
        assertEquals("Asia", results.get(0).get("Region"));
        assertEquals("Sub-Saharan Africa", results.get(1).get("Region"));
        assertEquals(8, ((Number) results.get(0).get("Total Units Sold")).intValue());
        assertEquals(10, ((Number) results.get(1).get("Total Units Sold")).intValue());
        assertEquals(results.get(0), response.get("aggregations"));
    }

    private static void createOrdersDatabase(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
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
                    )
                    """);
            try (PreparedStatement insert = conn.prepareStatement("""
                    INSERT INTO orders (
                        Region, Country, ItemType, SalesChannel, OrderPriority,
                        OrderDate, OrderID, ShipDate, UnitsSold, UnitPrice,
                        UnitCost, TotalRevenue, TotalCost, TotalProfit,
                        OrderProcessingTime, GrossMargin
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """)) {
                for (String line : Files.readAllLines(FIXTURES.resolve("expected").resolve("transformed_sales.csv")).subList(1, 4)) {
                    String[] values = line.split(",", -1);
                    insert.setString(1, values[0]);
                    insert.setString(2, values[1]);
                    insert.setString(3, values[2]);
                    insert.setString(4, values[3]);
                    insert.setString(5, values[4]);
                    insert.setString(6, values[5]);
                    insert.setInt(7, Integer.parseInt(values[6]));
                    insert.setString(8, values[7]);
                    insert.setInt(9, Integer.parseInt(values[8]));
                    insert.setDouble(10, Double.parseDouble(values[9]));
                    insert.setDouble(11, Double.parseDouble(values[10]));
                    insert.setDouble(12, Double.parseDouble(values[11]));
                    insert.setDouble(13, Double.parseDouble(values[12]));
                    insert.setDouble(14, Double.parseDouble(values[13]));
                    insert.setInt(15, Integer.parseInt(values[14]));
                    insert.setDouble(16, Double.parseDouble(values[15]));
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }
}
