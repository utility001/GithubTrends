# Airflow - GitHub Trends Data Pipeline

This folder contains Apache Airflow DAGs and utility scripts to orchestrate the ETL pipeline.

## DAG Overview

**DAG Name:** `github-trends-pipeline`

### Workflow Steps
+ `github-api-to-s3` - Gets data from the api and stages on s3 bucket
+ `s3-data-transform-and-reupload` - Ingest raw data from s3 bucket, transforms it and Reuploads it back into s3 buckeet
+ `create-table-if-not-exists` - Creates table on RDS database if it does not exist
+ `s3-to-rds` - Gets transformed data from S3 bucket and inserts into AWS RDS database

## files

| File | Description |
| - | - |
| `dags/github_trends_dag.py` | Main Airflow DAG definition |
| `dags/utils/api.py` | Contains Helper functions for extracting and transforming the data gotten from the API |
| `dags/utils/rds.py` | Contains Helper functions for RDS Database connection, Table creation and insert operation |
| `dags/utils/aws.py` | Contains Helper functions for AWS connection, Pushing data to S3 bucket and Getting data from S3 bucket |
| `dags/utils/etl.py` | Contains helper functions for ETL |
