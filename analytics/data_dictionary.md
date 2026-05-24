# Analytics Data Dictionary

## Data Sources

- `analytics/data/sales_kpis.csv`: Portfolio KPI rollup from `benchmark_inputs/100000SalesRecords.csv`.
- `analytics/data/regional_performance.csv`: Revenue, profit, margin, units, and order counts by region and country.
- `analytics/data/operations_processing_time.csv`: Average order processing days by region, country, item type, and sales channel.
- `analytics/data/lambda_benchmark_summary.csv`: Latest verified 1.5M-row Lambda benchmark summary.
- `analytics/data/optimization_story.csv`: Narrative milestones for the optimization dashboard page.

## Key Fields

- `total_revenue`: Sum of `Total Revenue` from the sales CSV.
- `total_profit`: Sum of `Total Profit` from the sales CSV.
- `gross_margin`: `total_profit / total_revenue`.
- `average_processing_days`: Difference between `Ship Date` and `Order Date`.
- `mean_runtime_seconds`: Lambda stage or pipeline runtime from SAAF metrics.
- `mean_throughput_rows_per_second`: Dataset rows divided by mean runtime.
- `estimated_cost_per_100k_usd`: Lambda cost estimate normalized to 100,000 invocations.

## Current KPI Baseline

- Orders: 100,000
- Revenue: $133,606,673,066.41
- Profit: $39,409,123,729.61
- Gross margin: 29.50%
- Units sold: 500,144,617
- Average processing time: 25.04 days
