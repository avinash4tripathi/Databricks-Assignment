# Databricks Assignment

This repository contains Databricks assignments organized by question.

## Repository Structure

```
Databricks-Assignment/
├── Question_1/                  # Medallion Architecture Pipeline
│   ├── source_to_bronze/         # Source to Bronze layer
│   │   ├── employee_source_to_bronze.py
│   │   └── utils.py              # Helper utilities
│   ├── bronze_to_silver/        # Bronze to Silver layer
│   │   └── employee_bronze_to_silver.py
│   └── silver_to_gold/          # Silver to Gold layer
│       └── employee_silver_to_gold.py
├── Question_2/                  # API Ingestion
│   ├── api.py
│   └── api_ingestion.py
└── README.md
```

## Question 1: Medallion Architecture (Bronze → Silver → Gold)

Implements a multi-hop data pipeline using the medallion architecture pattern:

- **Source to Bronze**: Ingests raw employee data from source files into the Bronze layer
- **Bronze to Silver**: Cleanses and transforms data from Bronze to Silver layer
- **Silver to Gold**: Aggregates and prepares final analytical views in the Gold layer

## Question 2: API Ingestion

Contains notebooks for ingesting data from external APIs into Databricks.

## Tech Stack

- Databricks
- PySpark
- Delta Lake
- Python

## Author

avinash4tripathi
