import boto3
import csv
import json
import logging
import os
from datetime import datetime
from Inspector import Inspector

# Initialize AWS S3 client
s3_client = boto3.client('s3')
transformed_csv_bucket_name = "tcss462-term-project"

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    inspector = Inspector()
    inspector.inspectAll()  # Start collecting runtime and system metrics

    # Log the event for debugging
    logger.info(event)

    # Safely access the body key
    body = event.get("body")
    if not body:
        raise KeyError("The 'body' key is missing from the event payload")

    # Parse the body based on its type
    if isinstance(body, str):
        body = json.loads(body)
    elif not isinstance(body, dict):
        raise TypeError("Unsupported body type in event: must be JSON string or dictionary")

    # Extract S3 bucket and file key from the body
    bucket_name = body["bucket_name"]
    file_key = body["key"]

    # Download the file from S3
    local_file_path = '/tmp/' + file_key
    s3_client.download_file(bucket_name, file_key, local_file_path)

    # Transform the file
    transformed_file_path = transform(local_file_path)

    # Upload the transformed file back to S3
    key = 'transformed_' + file_key
    with open(transformed_file_path, 'rb') as f:
        s3_client.put_object(Body=f, Bucket=transformed_csv_bucket_name, Key=key)

    inspector.inspectAllDeltas()  # Collect deltas for runtime metrics
    runtime_metrics = inspector.finish()  # Finalize the runtime metrics collection

    # Merge runtime_metrics at the top level of the JSON response
    response = {
        'statusCode': 200,
        'headers': {
            "Content-Type": "application/json"
        },
        'body': json.dumps({
            'bucket_name': transformed_csv_bucket_name,
            'key': key
        })
    }

    # Add all runtime metrics to the top-level response
    response.update(runtime_metrics)

    return response


def transform(file_path):
    """
    Transform the CSV file:
    - Add Order Processing Time
    - Transform Order Priority
    - Add Gross Margin
    - Remove duplicate rows based on Order ID
    """
    transformed_rows = []
    seen_order_ids = set()

    # Read the input CSV file
    with open(file_path, newline='', mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Skip duplicates
            if row['Order ID'] in seen_order_ids:
                continue

            # Mark Order ID as seen
            seen_order_ids.add(row['Order ID'])

            # Calculate Order Processing Time
            order_date = datetime.strptime(row['Order Date'], '%m/%d/%Y')
            ship_date = datetime.strptime(row['Ship Date'], '%m/%d/%Y')
            row['Order Processing Time'] = (ship_date - order_date).days

            # Transform Order Priority
            order_priority_mapping = {'H': 'High', 'C': 'Critical', 'L': 'Low', 'M': 'Medium'}
            row['Order Priority'] = order_priority_mapping.get(row['Order Priority'], 'Unknown')

            # Calculate Gross Margin
            total_profit = float(row['Total Profit'])
            total_revenue = float(row['Total Revenue'])
            row['Gross Margin'] = total_profit / total_revenue if total_revenue else 0

            # Add the transformed row
            transformed_rows.append(row)

    # Save the transformed data to a new CSV file in /tmp
    transformed_file_path = '/tmp/transformed_' + os.path.basename(file_path)
    with open(transformed_file_path, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames + ['Order Processing Time', 'Gross Margin'])
        writer.writeheader()
        writer.writerows(transformed_rows)

    return transformed_file_path
