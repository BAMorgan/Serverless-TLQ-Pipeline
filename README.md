# Serverless TLQ Pipeline - AWS Lambda
## Overview
This project implements a Transform-Load-Query (TLQ) data pipeline using AWS Lambda to process sales data in a serverless architecture. The pipeline is implemented in both Java and Python, allowing for a comparative analysis of performance, scalability, and cost-efficiency. The study follows the methodology outlined in the TCSS462 final project and explores the impact of multi-language serverless implementations in AWS.

## Features
- Transform Service: Processes raw CSV sales data, adds computed fields (e.g., Order Processing Time, Gross Margin), standardizes data, and removes duplicates.
- Load Service: Stores the transformed data in an SQLite database hosted in Amazon S3 for efficient querying.
- Query Service: Enables querying of sales data based on filters (e.g., Order Priority) and returns results in a structured format.
- Multi-Language Implementation: The pipeline is implemented in both Java and Python to analyze performance differences.
