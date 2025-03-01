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

        # Open SQLite database connection
        conn = sqlite3.connect(LOCAL_DB_PATH)
        cursor = conn.cursor()

        # Base queries for aggregation and filters
        sql_query_agg = '''
            SELECT 
                AVG(OrderProcessingTime) AS AvgOrderProcessingTime, 
                AVG(GrossMargin) AS AvgGrossMargin,
                AVG(CAST(UnitsSold AS FLOAT)) AS AvgUnitsSold,
                MAX(CAST(UnitsSold AS INTEGER)) AS MaxUnitsSold,
                MIN(CAST(UnitsSold AS INTEGER)) AS MinUnitsSold,
                SUM(CAST(UnitsSold AS INTEGER)) AS TotalUnitsSold,
                SUM(TotalRevenue) AS TotalRevenue,
                SUM(TotalProfit) AS TotalProfit,
                COUNT(DISTINCT OrderID) AS NumberOfOrders
            FROM 
                orders
        '''
        sql_query_filters = 'SELECT * FROM orders'

        # Dynamically build WHERE clause based on filters
        query_filters = []
        if filters.get("Region"):
            query_filters.append(f"Region = '{filters['Region']}'")
        if filters.get("Item Type"):
            query_filters.append(f"ItemType = '{filters['Item Type']}'")
        if filters.get("Sales Channel"):
            query_filters.append(f"SalesChannel = '{filters['Sales Channel']}'")
        if filters.get("Order Priority"):
            query_filters.append(f"OrderPriority = '{filters['Order Priority']}'")
        if filters.get("Country"):
            query_filters.append(f"Country = '{filters['Country']}'")

        # Add WHERE clause if filters exist
        if query_filters:
            where_clause = " WHERE " + " AND ".join(query_filters)
            sql_query_agg += where_clause
            sql_query_filters += where_clause

        # Add GROUP BY clause if provided
        if group_by:
            group_by_clause = " GROUP BY " + ", ".join(group_by)
            sql_query_agg += group_by_clause

        # Execute filter query
        logger.info(f"Executing filter query: {sql_query_filters}")
        cursor.execute(sql_query_filters)
        filter_result = cursor.fetchall()

        # Execute aggregation query
        logger.info(f"Executing aggregation query: {sql_query_agg}")
        cursor.execute(sql_query_agg)
        agg_result = cursor.fetchall()

        # Close database connection
        conn.close()

        # Add aggregation results (single dictionary)
        aggregations = {}
        if agg_result and agg_result[0]:
            aggregations = {
                "Average Order Processing Time in days": agg_result[0][0],
                "Average Gross Margin": agg_result[0][1],
                "Average Units Sold": agg_result[0][2],
                "Max Units Sold": agg_result[0][3],
                "Min Units Sold": agg_result[0][4],
                "Total Units Sold": agg_result[0][5],
                "Total Revenue": agg_result[0][6],
                "Total Profit": agg_result[0][7],
                "Number of Orders": int(agg_result[0][8])
            }

        # Finalize runtime metrics
        inspector.inspectAllDeltas()
        runtime_metrics = inspector.finish()

        # Merge runtime_metrics into the top-level response
        response = {
            "statusCode": 200,
            "aggregations": aggregations
        }
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
