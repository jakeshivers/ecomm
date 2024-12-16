
# E-Commerce Data Engineering Pipeline with Databricks

This repository contains a fully functional **E-commerce Data Engineering Pipeline** built using **Databricks**, designed to handle end-to-end workflows, from raw data ingestion to analytics-ready insights. The pipeline processes multiple data sources, applies transformations, and creates aggregate datasets for advanced business intelligence.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Architecture](#architecture)
4. [Technologies Used](#technologies-used)
5. [Setup and Installation](#setup-and-installation)
6. [Pipeline Breakdown](#pipeline-breakdown)
7. [Future Improvements](#future-improvements)
8. [Acknowledgments](#acknowledgments)

---

## Project Overview

This project simulates an e-commerce company’s data engineering needs, focusing on:

- Handling **real-time and batch data ingestion** from diverse sources.
- Using **Databricks Delta Lake** for scalable data storage and transformation.
- Creating **Silver Layer datasets** for insightful BI dashboards in tools like Tableau or Power BI.

The pipeline is designed with automation and monitoring to ensure reliable, efficient, and scalable workflows.

### Shopify Work:
- **Synthetic Data Creation**: Created synthetic data and sent it to **Stripe** and **Shopify** for integration testing.
- **ECS Data Creation Job**: Stored the data creation job in **ECS**, running it on a schedule for automated data generation.
- **Kestra Data Pipeline**: Utilized **Kestra** to orchestrate a Python job that pulls **Shopify** data, stores it in **S3** for processing in **Databricks**.

---

## Key Features

- **Multi-Source Ingestion**: Seamlessly integrates with APIs, JSON files, and databases.
- **Delta Lake Storage**: Implements Bronze and Silver Layers for raw and cleaned data.
- **Real-Time Transformations**: Supports near-real-time streaming for time-sensitive analytics.
- **Automation & Monitoring**: Uses Databricks Dashboards for scheduling and alerts for monitoring system health.
- **Interactive Analytics**: Enables live reporting through BI tools like ClickHouse, Tableau and Power BI.

---

## Architecture

![Pipeline Architecture Diagram](images\architecture.png)

- **Data Sources**: Simulated e-commerce data from APIs (transactions, inventory, shipping, and user analytics).
- **Databricks Workspace**: Used for data processing, storage, and orchestration.
- **Delta Lake**: Structured layers for raw, cleaned, and aggregated data.
- **BI Dashboards**: Live reporting on metrics such as revenue, inventory levels, and customer behavior.
- **ECS & Kestra Integration**: AWS ECS runs scheduled jobs for synthetic data creation, while Kestra manages the Python job for pulling and storing Shopify data for Databricks processing.

---

## Technologies Used

- **Databricks**: For processing, transformation, and scheduling.
- **Delta Lake**: To manage scalable, ACID-compliant storage layers.
- **Apache Spark**: For distributed data processing.
- **Python**: For ETL scripts and custom transformations.
- **APIs**: For ingesting transactional and analytical data.
- **Tableau/Power BI**: For data visualization and dashboards.
- **GitHub Actions**: For CI/CD pipeline integration.
- **AWS ECS**: To run scheduled jobs for synthetic data creation.
- **Kestra**: Orchestration of the Python job for pulling Shopify data into S3.

---

## Setup and Installation

### 1. Clone the Repository
```bash
git clone git@github.com:jakeshivers/ecomm.git
cd ecomm
```

### 2. Databricks Configuration:

- Create a Databricks workspace and cluster.
- Upload the notebooks in the /notebooks directory.

### 3. Dependencies: Install the required Python libraries using the requirements.txt file:

```bash
pip install -r requirements.txt
```

### 4. Environment Variables: Create a .env file with the following variables:

```env
API_KEY=<your_api_key>
DATABASE_URL=<your_database_url>
AWS_ACCESS_KEY=<your_aws_access_key>
AWS_SECRET=<your_aws_secret_key>
```

Update these variables as per your environment.

### 5. Run the Pipeline: Use the Databricks Jobs UI or API to schedule the pipeline notebooks.

---

## Pipeline Breakdown

### 1. Data Sources
- **Transactions API**: Customer purchases.
- **Inventory CSVs**: Stock levels from warehouse systems.
- **User Analytics**: Clickstream data.
- **Shopify API**: Data pulled from Shopify and processed via Kestra and Databricks.

### 2. Data Ingestion
- **Batch pulls**: For historical data.
- **Streaming ingestion**: For near-real-time processing.

### 3. Processing and Transformation
- **Deduplication**: Remove duplicate records.
- **Normalization and Enrichment**: Standardize and enhance raw data.
- **Aggregations**: Generate metrics like revenue trends and customer segmentation.

### 4. Data Storage
- **Bronze Layer**: Raw data stored in Delta Lake.
- **Silver Layer**: Cleaned and normalized data.
- **Gold Layer**: Aggregated datasets for analytics.

### 5. Analytics and Visualization
- **Revenue Dashboards**: Track daily revenue growth.
- **Inventory Heatmaps**: Visualize stock levels.
- **Customer Insights**: Segment customers based on spending patterns.

### 6. Automation and Monitoring
- **Job Scheduling**: Automate batch and streaming tasks.
- **Alerts**: Detect and respond to anomalies (e.g., API errors, inventory drops).

---

## Future Improvements

- **Enhance Streaming Capabilities**: Incorporate advanced streaming tools to achieve lower latency.
- **Advanced Analytics**: Implement predictive models for inventory forecasting and customer churn prediction.

## Acknowledgments
Data Engineer Camp for creating quality content and a wonderful learning environment.
