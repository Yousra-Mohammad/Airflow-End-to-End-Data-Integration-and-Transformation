# Airflow-End-to-End-Data-Integration-and-Transformation

## Overview:
This project demonstrates a comprehensive data integration and transformation pipeline using AWS RDS, Snowflake, and Airflow. It showcases the extraction of data from AWS RDS PostgreSQL, transformation using Python Pandas, and loading the transformed data into Snowflake as a data warehouse. The project also implements Slowly Changing Dimension (SCD) Type 2 to capture historical changes in employee wages.

## Features:
- Data extraction from AWS RDS PostgreSQL: Two tables, "salaries" from the "finance" schema and "employee_details" from the "HR" schema, are extracted as data sources.
- Data staging in AWS S3: Extracted data is loaded into AWS S3 as a staging area in CSV file format.
- Data transformation: Python Pandas is utilized to join the tables from the data source and identify updated rows and new records for insertion into the data warehouse.
- Data warehouse: Snowflake is used as the target data warehouse, incorporating Slowly Changing Dimension (SCD) Type 2 to track historical employee wages.
- Orchestration with Airflow: The ETL job is orchestrated using Airflow, with each step represented as a task within a Directed Acyclic Graph (DAG).

## Prerequisites:
- AWS RDS PostgreSQL instance with "finance" and "HR" schemas
- AWS S3 bucket for staging data
- Snowflake data warehouse instance
- Airflow installation and configuration

## Usage:
1. Ensure all the prerequisite services are running and accessible.
2. Trigger the Airflow DAG to initiate the ETL job.
3. Monitor the execution and logging within Airflow to track the progress and any potential issues.
4. Validate the transformed data in Snowflake and verify the successful implementation of Slowly Changing Dimension (SCD) Type 2.

