import sqlite3
import boto3
import json
import logging
from Inspector import Inspector  # Ensure the Inspector class is available in your Lambda deployment package

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

# Path for the SQLite database in Lambda's local storage
LOCAL_DB_PATH = "/tmp/data.db"

FILTER_COLUMNS = {
    "Region": "Region",
    "Country": "Country",
    "Item Type": "ItemType",
    "Sales Channel": "SalesChannel",
    "Order Priority": "OrderPriority",
}


def _dedupe_group_columns(group_by):
    seen = set()
    group_columns = []
    for column in group_by or []:
        if column not in FILTER_COLUMNS:
            raise ValueError(f"Unsupported group by column: {column}")
        db_column = FILTER_COLUMNS[column]
        if db_column not in seen:
            seen.add(db_column)
            group_columns.append(db_column)
    return group_columns


def build_aggregation_query(filters=None, group_by=None):
    filters = filters or {}
    group_columns = _dedupe_group_columns(group_by or [])

    select_columns = [
        "ROUND(AVG(OrderProcessingTime), 2) AS AvgOrderProcessingTime",
        "ROUND(AVG(GrossMargin), 4) AS AvgGrossMargin",
        "ROUND(AVG(CAST(UnitsSold AS FLOAT)), 2) AS AvgUnitsSold",
        "MAX(CAST(UnitsSold AS INTEGER)) AS MaxUnitsSold",
        "MIN(CAST(UnitsSold AS INTEGER)) AS MinUnitsSold",
        "SUM(CAST(UnitsSold AS INTEGER)) AS TotalUnitsSold",
        "ROUND(SUM(TotalRevenue), 2) AS TotalRevenue",
        "ROUND(SUM(TotalProfit), 2) AS TotalProfit",
        "COUNT(DISTINCT OrderID) AS NumberOfOrders",
    ]
    select_columns.extend(group_columns)

    query = f"SELECT {', '.join(select_columns)} FROM orders"
    where_clauses = []
    params = []
    for column, value in filters.items():
        if column not in FILTER_COLUMNS:
            raise ValueError(f"Unsupported filter column: {column}")
        where_clauses.append(f"{FILTER_COLUMNS[column]} = ?")
        params.append(value)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    if group_columns:
        query += " GROUP BY " + ", ".join(group_columns)
        query += " ORDER BY " + ", ".join(group_columns)

    return query, params, group_columns


def format_aggregation(row, group_columns=None):
    group_columns = group_columns or []
    aggregation = {
        "Average Order Processing Time in days": row["AvgOrderProcessingTime"],
        "Average Gross Margin": row["AvgGrossMargin"],
        "Average Units Sold": row["AvgUnitsSold"],
        "Max Units Sold": row["MaxUnitsSold"],
        "Min Units Sold": row["MinUnitsSold"],
        "Total Units Sold": row["TotalUnitsSold"],
        "Total Revenue": row["TotalRevenue"],
        "Total Profit": row["TotalProfit"],
        "Number of Orders": int(row["NumberOfOrders"]),
    }
    for column in group_columns:
        aggregation[column] = row[column]
    return aggregation


def lambda_handler(event, context):
    inspector = Inspector()
    inspector.inspectAll()  # Start collecting runtime and system metrics

    try:
        logger.info("Lambda function started.")
        logger.info(f"Received event: {json.dumps(event, indent=2)}")

        # Extract bucket and key for database file
        bucket_name = event.get("bucket_name")
        db_key = event.get("key")

        if not bucket_name or not db_key:
            raise ValueError("Both 'bucket_name' and 'key' are required.")

        # Ensure the file is saved in the `/tmp` directory
        logger.info(f"Downloading database from S3: {bucket_name}/{db_key}")
        s3_client.download_file(bucket_name, db_key, LOCAL_DB_PATH)
        logger.info("Database file downloaded successfully.")

        # Extract filters and group by from the request
        filters = event.get("Filters", {})
        group_by = event.get("Group By", [])

        # Execute aggregation query
        query, params, group_columns = build_aggregation_query(filters, group_by)
        logger.info(f"Executing aggregation query: {query}")
        with sqlite3.connect(LOCAL_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()

        results = [format_aggregation(row, group_columns) for row in rows]
        aggregations = results[0] if results else {}

        # Finalize runtime metrics
        inspector.inspectAllDeltas()
        runtime_metrics = inspector.finish()

        # Merge runtime_metrics into the top-level response
        response = {
            "statusCode": 200,
            "aggregations": aggregations
        }
        if group_columns:
            response["results"] = results
        response.update(runtime_metrics)  # Add runtime metrics to the top level

        logger.info(f"Response: {response}")
        return response

    except Exception as e:
        inspector.inspectAllDeltas()  # Collect deltas even on failure
        runtime_metrics = inspector.finish()  # Finalize the runtime metrics collection
        logger.error(f"An error occurred: {e}", exc_info=True)
        error_response = {
            "statusCode": 500,
            "error": str(e)
        }
        error_response.update(runtime_metrics)  # Add runtime metrics to the error response
        return error_response
