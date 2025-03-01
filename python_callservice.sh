#!/bin/bash

# Configuration
NUM_ROWS=10000  # Number of rows
dataset_size=$NUM_ROWS  # Adjust dataset size
iterations=10     # Number of iterations
input_file="${NUM_ROWS}SalesRecords.csv"
output_csv="pipeline_results.csv"

# Summary arrays
transform_times=()
load_times=()
query_times=()
total_times=()
transform_throughputs=()
load_throughputs=()
query_throughputs=()
total_throughputs=()

# Helper function for percentiles
percentile() {
    local p=$1
    shift
    local values=($(echo "$@" | tr " " "\n" | sort -n))
    local count=${#values[@]}
    local index=$(echo "$p/100 * ($count-1)" | bc -l | awk '{printf "%.0f", $0}')
    echo "${values[$index]}"
}

# Function to run Lambda and collect metrics
run_lambda() {
    local lambda_name="$1"
    local payload="$2"

    aws lambda invoke --function-name "$lambda_name" --payload "$payload" response.json --cli-binary-format raw-in-base64-out > /dev/null
    runtime=$(jq -r '.runtime' response.json)
    runtime_sec=$(echo "scale=3; $runtime / 1000" | bc)
    echo "$runtime_sec"
}

# Initialize CSV file
echo "Iteration,Transform Time,Transform Throughput,Load Time,Load Throughput,Query Time,Query Throughput,Total Time,Total Throughput" > "$output_csv"

echo "Starting pipeline tests with configuration:"
echo "Dataset size: $dataset_size rows"
echo "Number of iterations: $iterations"
echo "Input file: $input_file"
echo "----------------------------------------"

# Actual iteration loop
for ((iteration=1; iteration<=iterations; iteration++)); do
    echo "Running pipeline iteration $iteration..."
    
    echo "Running Transform function - iteration $iteration"
    transform_time=$(run_lambda "pythonTransform" '{
      "body": {
        "bucket_name": "tcss462-term-project",
        "key": "'"$input_file"'"
      }
    }')
    throughput_transform=$(echo "scale=2; $dataset_size / $transform_time" | bc)

    echo "Running Load function - iteration $iteration"
    load_time=$(run_lambda "pythonLoad" '{
      "bucket_name": "tcss462-term-project",
      "key": "transformed_'"$NUM_ROWS"'SalesRecords.csv",
      "db_file_name": "data.db"
    }')
    throughput_load=$(echo "scale=2; $dataset_size / $load_time" | bc)

    echo "Running Query function - iteration $iteration"
    query_time=$(run_lambda "pythonQuery" '{
      "bucket_name": "tcss462-term-project",
      "key": "databases/data.db",
      "Filters": {
        "Region": "Sub-Saharan Africa"
      },
      "Group By": [
        "Region",
        "Country"
      ]
    }')
    throughput_query=$(echo "scale=2; $dataset_size / $query_time" | bc)

    # Total time and throughput
    total_time=$(echo "$transform_time + $load_time + $query_time" | bc)
    throughput_total=$(echo "scale=2; $dataset_size / $total_time" | bc)

    # Save metrics for summary
    transform_times+=("$transform_time")
    load_times+=("$load_time")
    query_times+=("$query_time")
    total_times+=("$total_time")
    transform_throughputs+=("$throughput_transform")
    load_throughputs+=("$throughput_load")
    query_throughputs+=("$throughput_query")
    total_throughputs+=("$throughput_total")

    # Append metrics to CSV file
    echo "$iteration,$transform_time,$throughput_transform,$load_time,$throughput_load,$query_time,$throughput_query,$total_time,$throughput_total" >> "$output_csv"

    # Display metrics for each iteration
    echo "Metrics for iteration $iteration:"
    printf "Transform: %.3fs (%s rows/sec)\n" $transform_time $throughput_transform
    printf "Load: %.3fs (%s rows/sec)\n" $load_time $throughput_load
    printf "Query: %.3fs (%s rows/sec)\n" $query_time $throughput_query
    printf "Total: %.3fs (%s rows/sec)\n" $total_time $throughput_total
    echo "----------------------------------------"
done

# Calculate summary statistics
calc_summary() {
    local name=$1
    shift
    local values=("$@")
    local count=${#values[@]}
    local avg=$(echo "${values[@]}" | tr " " "\n" | awk '{sum+=$1} END {print sum/NR}')
    local stddev=$(echo "${values[@]}" | tr " " "\n" | awk -v avg="$avg" '{sum+=($1-avg)^2} END {print sqrt(sum/NR)}')
    local min=$(echo "${values[@]}" | tr " " "\n" | sort -n | head -1)
    local max=$(echo "${values[@]}" | tr " " "\n" | sort -n | tail -1)
    local pct95=$(percentile 95 "${values[@]}")

    echo "$name:"
    printf "  Average: %.2f\n" $avg
    printf "  Std Dev: %.2f\n" $stddev
    printf "  Min: %.2f\n" $min
    printf "  Max: %.2f\n" $max
    printf "  95th percentile: %.2f\n" $pct95
}

echo
echo "Pipeline Statistics for $dataset_size rows:"
echo
echo "Throughput Statistics (rows/second):"
calc_summary "Transform" "${transform_throughputs[@]}"
calc_summary "Load" "${load_throughputs[@]}"
calc_summary "Query" "${query_throughputs[@]}"
calc_summary "Total" "${total_throughputs[@]}"

echo "Results saved to $output_csv"
