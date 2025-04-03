# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bc5c5ff7-dbfe-46f0-af4c-e5b298762e86",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "8d3001ae-2f0d-4d23-a9ee-f2698798f695"
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
import requests
import time
import logging
from io import StringIO
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, to_date, lit, current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BronzeDataLoader")

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
CONFIG = {
    "source": {
        "base_url": "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets",
        "systems": {
            "crm": ["cust_info", "prd_info", "sales_details"],
            "erp": ["CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"]
        },
        "max_retries": 3,
        "retry_delay": 2  # seconds
    },
    "target": {
        "database": "bronze",
        "partition_column": "ingestion_date",
        "sample_rows": 3
    },
    "performance": {
        "batch_size": 10000,  # For larger files
        "use_arrow": True
    }
}

# Define schemas for all tables to match SQL Server definitions
SCHEMAS = {
    "cust_info": StructType([
        StructField("cst_id", IntegerType(), True),
        StructField("cst_key", StringType(), True),
        StructField("cst_firstname", StringType(), True),
        StructField("cst_lastname", StringType(), True),
        StructField("cst_marital_status", StringType(), True),
        StructField("cst_gndr", StringType(), True),
        StructField("cst_create_date", DateType(), True)
    ]),
    "prd_info": StructType([
        StructField("prd_id", IntegerType(), True),
        StructField("prd_key", StringType(), True),
        StructField("prd_nm", StringType(), True),
        StructField("prd_cost", IntegerType(), True),
        StructField("prd_line", StringType(), True),
        StructField("prd_start_dt", DateType(), True),
        StructField("prd_end_dt", DateType(), True)
    ]),
    "sales_details": StructType([
        StructField("sls_ord_num", StringType(), True),
        StructField("sls_prd_key", StringType(), True),
        StructField("sls_cust_id", IntegerType(), True),
        StructField("sls_order_dt", IntegerType(), True),
        StructField("sls_ship_dt", IntegerType(), True),
        StructField("sls_due_dt", IntegerType(), True),
        StructField("sls_sales", IntegerType(), True),
        StructField("sls_quantity", IntegerType(), True),
        StructField("sls_price", IntegerType(), True)
    ]),
    "CUST_AZ12": StructType([
        StructField("cid", StringType(), True),
        StructField("bdate", DateType(), True),
        StructField("gen", StringType(), True)
    ]),
    "LOC_A101": StructType([
        StructField("cid", StringType(), True),
        StructField("cntry", StringType(), True)
    ]),
    "PX_CAT_G1V2": StructType([
        StructField("id", StringType(), True),
        StructField("cat", StringType(), True),
        StructField("subcat", StringType(), True),
        StructField("maintenance", StringType(), True)
    ])
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_data(url, max_retries=CONFIG["source"]["max_retries"], retry_delay=CONFIG["source"]["retry_delay"]):
    """
    Fetch data from URL with retry logic

    Args:
        url (str): URL to fetch data from
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Seconds to wait between retries

    Returns:
        str: Content of the response if successful

    Raises:
        Exception: If all retry attempts fail
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            return response.text
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"All {max_retries} attempts failed for {url}")
                raise

def preprocess_dataframe(pdf, file_name):
    """
    Apply file-specific preprocessing to pandas DataFrame

    Args:
        pdf (DataFrame): Pandas DataFrame to preprocess
        file_name (str): Name of the file being processed

    Returns:
        DataFrame: Preprocessed pandas DataFrame
    """
    # Standardize column names to lowercase for all files
    pdf.columns = [col.lower() for col in pdf.columns]

    # Apply file-specific transformations
    if file_name.lower() == "cust_info":
        pdf['cst_id'] = pd.to_numeric(pdf['cst_id'], errors='coerce')
        if 'cst_create_date' in pdf.columns:
            pdf['cst_create_date'] = pd.to_datetime(pdf['cst_create_date'], errors='coerce')

    elif file_name.lower() == "prd_info":
        pdf['prd_id'] = pd.to_numeric(pdf['prd_id'], errors='coerce')
        pdf['prd_cost'] = pd.to_numeric(pdf['prd_cost'], errors='coerce')
        pdf['prd_start_dt'] = pd.to_datetime(pdf['prd_start_dt'], errors='coerce')
        pdf['prd_end_dt'] = pd.to_datetime(pdf['prd_end_dt'], errors='coerce')

    elif file_name.lower() == "sales_details":
        numeric_cols = ['sls_cust_id', 'sls_order_dt', 'sls_ship_dt', 'sls_due_dt',
                       'sls_sales', 'sls_quantity', 'sls_price']
        for col in numeric_cols:
            if col in pdf.columns:
                pdf[col] = pd.to_numeric(pdf[col], errors='coerce')

    elif file_name.upper() == "CUST_AZ12":
        if 'bdate' in pdf.columns:
            pdf['bdate'] = pd.to_datetime(pdf['bdate'], errors='coerce')

    # Add data quality checks
    null_counts = pdf.isnull().sum()
    if null_counts.any():
        logger.info(f"Null value counts in {file_name}: {null_counts[null_counts > 0].to_dict()}")

    return pdf

def convert_to_spark_df(pdf, file_name):
    """
    Convert pandas DataFrame to Spark DataFrame with appropriate schema

    Args:
        pdf (DataFrame): Pandas DataFrame to convert
        file_name (str): Name of the file being processed

    Returns:
        DataFrame: Spark DataFrame
    """
    # Use predefined schema if available, otherwise infer
    if file_name in SCHEMAS:
        schema = SCHEMAS[file_name]
        logger.info(f"Using predefined schema for {file_name}")
        sdf = spark.createDataFrame(pdf, schema=schema)
    else:
        logger.info(f"Inferring schema for {file_name}")
        sdf = spark.createDataFrame(pdf)

    # Add metadata columns
    sdf = sdf.withColumn("ingestion_date", current_timestamp())
    sdf = sdf.withColumn("source_file", lit(file_name))

    return sdf

def write_to_bronze(sdf, table_name):
    """
    Write Spark DataFrame to bronze layer as Delta table

    Args:
        sdf (DataFrame): Spark DataFrame to write
        table_name (str): Target table name

    Returns:
        int: Number of rows written
    """
    # Write to Delta table with partitioning
    partition_col = CONFIG["target"]["partition_column"]

    sdf.write.format("delta") \
        .mode("overwrite") \
        .partitionBy(partition_col) \
        .saveAsTable(table_name)

    # Get row count for verification
    count = spark.table(table_name).count()
    return count

def load_to_bronze(file_list, system):
    """
    Main function to load data from source to bronze layer

    Args:
        file_list (list): List of files to process
        system (str): Source system name (crm, erp)

    Returns:
        dict: Summary of processing results
    """
    results = {}
    base_url = CONFIG["source"]["base_url"]

    for file in file_list:
        start_time = time.time()
        url = f"{base_url}/source_{system.lower()}/{file}.csv"
        table_name = f"{CONFIG['target']['database']}.{system.lower()}_{file.lower()}"

        logger.info(f"Processing: {url} -> {table_name}")

        try:
            # Step 1: Fetch data
            content = fetch_data(url)

            # Step 2: Load into pandas
            pdf = pd.read_csv(StringIO(content))
            raw_count = len(pdf)
            logger.info(f"Downloaded {raw_count} rows from {url}")

            # Step 3: Preprocess data
            pdf = preprocess_dataframe(pdf, file)

            # Step 4: Convert to Spark DataFrame
            sdf = convert_to_spark_df(pdf, file)

            # Step 5: Write to bronze table
            count = write_to_bronze(sdf, table_name)

            # Step 6: Verify and log results
            elapsed_time = time.time() - start_time
            logger.info(f"✅ Loaded {count} rows into {table_name} in {elapsed_time:.2f} seconds")

            # Show sample data
            sample_rows = CONFIG["target"]["sample_rows"]
            logger.info(f"Sample data from {table_name}:")
            spark.table(table_name).show(sample_rows, truncate=False)

            # Store results
            results[file] = {
                "status": "success",
                "rows": count,
                "time": elapsed_time,
                "table": table_name
            }

        except Exception as e:
            logger.error(f"❌ Error processing {url}: {str(e)}")
            results[file] = {
                "status": "error",
                "error": str(e),
                "table": table_name
            }

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set Spark configurations for better performance
if CONFIG["performance"]["use_arrow"]:
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

# Track overall execution time
overall_start_time = time.time()

# Process all source systems
all_results = {}

for system, files in CONFIG["source"]["systems"].items():
    logger.info(f"\n== Loading {system.upper()} Data ==")
    system_results = load_to_bronze(files, system)
    all_results[system] = system_results

# Calculate summary statistics
total_tables = sum(len(system_results) for system_results in all_results.values())
successful_tables = sum(
    sum(1 for result in system_results.values() if result["status"] == "success")
    for system_results in all_results.values()
)
total_rows = sum(
    sum(result.get("rows", 0) for result in system_results.values())
    for system_results in all_results.values()
)

# Log summary
overall_time = time.time() - overall_start_time
logger.info(f"\n=== Bronze Data Load Summary ===")
logger.info(f"Total execution time: {overall_time:.2f} seconds")
logger.info(f"Tables processed: {successful_tables}/{total_tables}")
logger.info(f"Total rows loaded: {total_rows}")

if successful_tables == total_tables:
    logger.info("✅ All bronze data loaded successfully")
else:
    logger.warning(f"⚠️ {total_tables - successful_tables} tables failed to load")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
