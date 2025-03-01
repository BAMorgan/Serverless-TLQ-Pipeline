#!/bin/bash

# Configuration
BUCKET_NAME="tcss462-term-project"
NUM_ROWS=10000  # Variable for number of rows
NUM_ITERATIONS=10
OUTPUT_FILE="java_pipeline_metrics.csv"
TEMP_METRICS="/tmp/pipeline_metrics.tmp"

# Derive input file name from NUM_ROWS
INPUT_FILE="${NUM_ROWS}SalesRecords.csv"

# Create CSV header with units in a temporary file first
echo "iteration,test_type,transform_duration,load_duration,query_duration,total_duration,transform_throughput,load_throughput,query_throughput,total_throughput" > $TEMP_METRICS

# Function to invoke Lambda and extract duration
invoke_lambda() {
    local function_name=$1
    local payload=$2
    
    # Invoke Lambda and capture entire response
    local response=$(aws lambda invoke \
        --function-name $function_name \
        --payload "$payload" \
        --cli-binary-format raw-in-base64-out \
        --log-type Tail \
        /dev/stdout)
    
    # Extract duration from the response using proper grep syntax
    if [ $? -eq 0 ]; then
        local duration=$(echo "$response" | grep -o "\"runtime\":[0-9]*" | grep -o "[0-9]*")
        if [ -z "$duration" ]; then
            echo "0"
        else
            echo "$duration"
        fi
    else
        echo "0"
    fi
}

# Function to calculate throughput (rows per second)
calculate_throughput() {
    local duration_ms=$1
    if [ "$duration_ms" -eq 0 ]; then
        echo "0"
    else
        # Convert duration to seconds and calculate rows per second
        echo "scale=2; $NUM_ROWS / ($duration_ms / 1000)" | bc
    fi
}

# Function to run one complete pipeline iteration
run_pipeline() {
    local iteration=$1
    local metrics_line
    
    # Transform
    echo "Running Transform function - iteration $iteration (Dataset size: ${NUM_ROWS} rows)"
    local transform_payload="{\"body\": {\"bucket_name\": \"$BUCKET_NAME\", \"key\": \"$INPUT_FILE\"}}"
    local transform_duration=$(invoke_lambda "javaTransform" "$transform_payload")
    local transform_throughput=$(calculate_throughput $transform_duration)
    
    # Load
    echo "Running Load function - iteration $iteration"
    local load_payload="{\"bucket_name\": \"$BUCKET_NAME\", \"csv_file_key\": \"transformed_$INPUT_FILE\", \"db_file_name\": \"data_${iteration}.db\"}"
    local load_duration=$(invoke_lambda "javaLoad" "$load_payload")
    local load_throughput=$(calculate_throughput $load_duration)
    
    # Query
    echo "Running Query function - iteration $iteration"
    local query_payload="{\"bucket_name\": \"$BUCKET_NAME\", \"key\": \"databases/data.db\", \"Filters\": {\"Region\": \"Sub-Saharan Africa\"}, \"Group By\": [\"Region\", \"Country\"]}"
    local query_duration=$(invoke_lambda "javaQuery" "$query_payload")
    local query_throughput=$(calculate_throughput $query_duration)
    
    # Calculate total duration and throughput
    local total_duration=$((transform_duration + load_duration + query_duration))
    local total_throughput=$(calculate_throughput $total_duration)
    
    # Convert durations to seconds for display
    local transform_seconds=$(echo "scale=3; $transform_duration/1000" | bc)
    local load_seconds=$(echo "scale=3; $load_duration/1000" | bc)
    local query_seconds=$(echo "scale=3; $query_duration/1000" | bc)
    local total_seconds=$(echo "scale=3; $total_duration/1000" | bc)
    
    # Create metrics line
    metrics_line="$iteration,warm,$transform_duration,$load_duration,$query_duration,$total_duration,$transform_throughput,$load_throughput,$query_throughput,$total_throughput"
    
    # Append to temporary file
    echo "$metrics_line" >> $TEMP_METRICS
    
    # Debug output
    echo "Metrics for iteration $iteration:"
    echo "Transform: ${transform_seconds}s (${transform_throughput} rows/sec)"
    echo "Load: ${load_seconds}s (${load_throughput} rows/sec)"
    echo "Query: ${query_seconds}s (${query_throughput} rows/sec)"
    echo "Total: ${total_seconds}s (${total_throughput} rows/sec)"
    echo "----------------------------------------"
}

# Print configuration
echo "Starting pipeline tests with configuration:"
echo "Dataset size: ${NUM_ROWS} rows"
echo "Number of iterations: ${NUM_ITERATIONS}"
echo "Input file: ${INPUT_FILE}"
echo "----------------------------------------"

# Execute iterations
echo "Starting pipeline tests..."
for ((i=1; i<=$NUM_ITERATIONS; i++)); do
    run_pipeline $i
done

# Copy temporary file to final location
cp $TEMP_METRICS $OUTPUT_FILE

# Generate summary statistics
echo "Generating summary statistics..."
python3 - << EOF
import pandas as pd
import numpy as np

try:
    # Read the data
    df = pd.read_csv('$OUTPUT_FILE')
    
    # Convert durations from milliseconds to seconds for display
    duration_cols = ['transform_duration', 'load_duration', 'query_duration', 'total_duration']
    duration_display = df[duration_cols].copy() / 1000  # Convert to seconds
    throughput_cols = ['transform_throughput', 'load_throughput', 'query_throughput', 'total_throughput']
    
    # Calculate statistics
    print(f"\nPipeline Statistics for {$NUM_ROWS} rows:")
    
    # Duration Statistics
    print("\nDuration Statistics (in seconds):")
    for col in duration_cols:
        stats = duration_display[col].agg(['mean', 'std', 'min', 'max'])
        p95 = duration_display[col].quantile(0.95)
        name = col.replace('_duration', '').title()
        print(f"\n{name}:")
        print(f"  Average: {stats['mean']:.3f}")
        print(f"  Std Dev: {stats['std']:.3f}")
        print(f"  Min: {stats['min']:.3f}")
        print(f"  Max: {stats['max']:.3f}")
        print(f"  95th percentile: {p95:.3f}")
    
    # Throughput Statistics
    print("\nThroughput Statistics (rows/second):")
    for col in throughput_cols:
        stats = df[col].agg(['mean', 'std', 'min', 'max'])
        p95 = df[col].quantile(0.95)
        name = col.replace('_throughput', '').title()
        print(f"\n{name}:")
        print(f"  Average: {stats['mean']:.2f}")
        print(f"  Std Dev: {stats['std']:.2f}")
        print(f"  Min: {stats['min']:.2f}")
        print(f"  Max: {stats['max']:.2f}")
        print(f"  95th percentile: {p95:.2f}")
    
    # Save detailed statistics to CSV
    stats_dict = {}
    for col in duration_cols:
        name = f"{col.replace('_duration', '')} (seconds)"
        stats_dict[name] = duration_display[col].agg(['mean', 'std', 'min', 'max', 
            lambda x: x.quantile(0.95)]).round(3)
    
    for col in throughput_cols:
        name = f"{col.replace('_throughput', '')} (rows/sec)"
        stats_dict[name] = df[col].agg(['mean', 'std', 'min', 'max', 
            lambda x: x.quantile(0.95)]).round(2)
    
    stats_df = pd.DataFrame(stats_dict)
    stats_df.index = ['Mean', 'Std Dev', 'Min', 'Max', '95th Percentile']
    stats_df.to_csv('summary_statistics.csv')
            
except Exception as e:
    print(f"Error processing statistics: {str(e)}")
    print("DataFrame columns:", df.columns.tolist() if 'df' in locals() else "DataFrame not loaded")
EOF

# Cleanup
rm -f $TEMP_METRICS