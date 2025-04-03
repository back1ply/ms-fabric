# Simplified Data Warehouse Project

This project demonstrates a streamlined data warehouse implementation using Microsoft Fabric. It follows the Medallion Architecture with Bronze, Silver, and Gold layers, but with a simplified approach focused on essential functionality.

## Project Structure

The project consists of three main notebooks and a data pipeline:

1. **00_load_bronze_data**: Downloads data from GitHub and loads it directly into bronze tables
2. **01_transform_bronze_to_silver**: Performs essential quality checks and transforms bronze data to silver layer
3. **02_transform_silver_to_gold**: Transforms silver data to gold layer for analytics
4. **load_data_warehouse**: Pipeline that orchestrates the execution of the notebooks

## Data Architecture

The data architecture follows the Medallion Architecture pattern:

- **Bronze Layer**: Raw data ingested from source systems (CRM and ERP)
- **Silver Layer**: Cleansed and standardized data
- **Gold Layer**: Business-ready data modeled for analytics

## Simplifications Made

This project is a streamlined version of a more complex implementation. Key simplifications include:

1. **Combined Data Loading**: Merged HTTP download and table loading into a single notebook
2. **Focused Quality Checks**: Integrated essential quality checks directly into the transformation process
3. **Simplified Transformations**: Used SQL for better readability where appropriate
4. **Enhanced Gold Layer**: Added derived metrics and a summary view for quick analysis
5. **Streamlined Pipeline**: Created a straightforward pipeline with notebook activities

## Running the Project

To run the project:

1. Open the `load_data_warehouse` pipeline
2. Click "Run" to execute the entire data pipeline
3. Alternatively, run each notebook individually in sequence

## Data Model

The gold layer contains the following tables:

- **dim_customers**: Customer dimension with integrated data from CRM and ERP
- **dim_products**: Product dimension with category information
- **fact_sales**: Sales fact table with order details and metrics
- **vw_sales_summary**: Summary view for quick analysis by category, country, and time

## Benefits of This Approach

- **Reduced Complexity**: Fewer notebooks and simpler code
- **Improved Readability**: SQL used where appropriate for better readability
- **Enhanced Analytics**: Added derived metrics and a summary view
- **Streamlined Pipeline**: Simpler orchestration with clear dependencies
