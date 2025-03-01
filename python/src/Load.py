import boto3
import os
import sqlite3
import csv
import json
import logging
from Inspector import Inspector  # Ensure the Inspector class is available in your Lambda deployment package

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    inspector = Inspector()
    inspector.inspectAll()  # Start collecting runtime and system metrics

    try:
        logger.info("Lambda function started.")
        logger.info(f"Received event: {json.dumps(event, indent=2)}")

        # Extract parameters from the event
        bucket_name = event.get("bucket_name")
        csv_file_key = event.get("key")
        db_file_name = event.get("db_file_name", "data.db")  # Default file name: data.db

        if not bucket_name or not csv_file_key:
            raise ValueError("Both 'bucket_name' and 'csv_file_key' are required.")

        # Paths for local files
        local_csv_path = f"/tmp/{os.path.basename(csv_file_key)}"
        local_db_path = f"/tmp/{db_file_name}"

        # Download the CSV file from S3
        logger.info(f"Downloading CSV file from S3: {bucket_name}/{csv_file_key}")
        s3_client.download_file(bucket_name, csv_file_key, local_csv_path)
        logger.info(f"CSV file downloaded successfully to {local_csv_path}")

        # Check if the SQLite database exists; if not, create it
        conn = sqlite3.connect(local_db_path)
        cursor = conn.cursor()

        # Create a table if it doesn't exist
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Region TEXT,
                Country TEXT,
                ItemType TEXT,
                SalesChannel TEXT,
                OrderPriority TEXT,
                OrderDate TEXT,
                OrderID INTEGER,
                ShipDate TEXT,
                UnitsSold INTEGER,
                UnitPrice REAL,
                UnitCost REAL,
                TotalRevenue REAL,
                TotalCost REAL,
                TotalProfit REAL,
                OrderProcessingTime INTEGER,
                GrossMargin REAL
            );
        '''
        cursor.execute(create_table_query)
        conn.commit()

        # Read and insert CSV data into the SQLite database
        logger.info(f"Reading CSV file and inserting data into SQLite database: {local_db_path}")
        with open(local_csv_path, newline='', mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            insert_query = '''
                INSERT INTO orders (
                    Region, Country, ItemType, SalesChannel, OrderPriority,
                    OrderDate, OrderID, ShipDate, UnitsSold, UnitPrice,
                    UnitCost, TotalRevenue, TotalCost, TotalProfit,
                    OrderProcessingTime, GrossMargin
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            data_to_insert = [
                (
                    row['Region'], row['Country'], row['Item Type'], row['Sales Channel'], row['Order Priority'],
                    row['Order Date'], int(row['Order ID']), row['Ship Date'], int(row['Units Sold']), float(row['Unit Price']),
                    float(row['Unit Cost']), float(row['Total Revenue']), float(row['Total Cost']), float(row['Total Profit']),
                    int(row['Order Processing Time']), float(row['Gross Margin'])
                ) for row in reader
            ]
            cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        conn.close()
        logger.info(f"Data inserted successfully into SQLite database.")

        # Upload the SQLite database to S3
        s3_key = f"databases/{db_file_name}"  # S3 key for the database file
        logger.info(f"Uploading SQLite database to S3: {bucket_name}/{s3_key}")

        with open(local_db_path, "rb") as db_file:
            s3_client.put_object(Body=db_file, Bucket=bucket_name, Key=s3_key)

        logger.info("Database uploaded successfully.")

        inspector.inspectAllDeltas()  # Collect deltas for runtime metrics
        runtime_metrics = inspector.finish()  # Finalize the runtime metrics collection

        # Merge runtime_metrics into the top-level response
        response = {
            "statusCode": 200,
            "message": "Database created and uploaded successfully",
            "bucket_name": bucket_name,
            "s3_key": s3_key
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

