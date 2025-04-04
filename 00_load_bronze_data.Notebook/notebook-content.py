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

# ============================================================
# BRONZE DATA LOADER NOTEBOOK
# ============================================================
# This notebook loads data from CSV files hosted on GitHub into the Bronze layer
# of our data lakehouse. The Bronze layer stores raw data with minimal transformations.
#
# What this notebook does:
# 1. Connects to GitHub to download CSV files
# 2. Loads the data into pandas DataFrames
# 3. Performs basic data type conversions
# 4. Converts to Spark DataFrames
# 5. Writes the data to Delta tables in the Bronze layer
# ============================================================

# Import required libraries
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

# ============================================================
# SETUP LOGGING AND SPARK SESSION
# ============================================================
# Set up basic logging to track what's happening in our notebook
# and initialize our Spark session

# Configure simple logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BronzeDataLoader")

# Create a Spark session - this is our connection to Spark
spark = SparkSession.builder.getOrCreate()

# ============================================================
# CONFIGURATION
# ============================================================
# This section defines where we get data from and where we save it

# Configuration settings for our data loading process
CONFIG = {
    # Source configuration - where we get the data from
    "source": {
        "base_url": "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets",
        # Systems and files we want to load
        "systems": {
            "crm": ["cust_info", "prd_info", "sales_details"],
            "erp": ["CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"]
        },
        "max_retries": 3,  # How many times to retry if download fails
        "retry_delay": 2   # Seconds to wait between retries
    },
    # Target configuration - where we save the data
    "target": {
        "database": "bronze",  # Database name in the lakehouse
        "partition_column": "ingestion_date",  # Column to partition data by
        "sample_rows": 3  # Number of sample rows to display
    },
    # Performance settings
    "performance": {
        "batch_size": 10000,  # For larger files
        "use_arrow": True  # Use Apache Arrow for faster pandas-to-Spark conversion
    }
}

# ============================================================
# DATA SCHEMAS
# ============================================================
# Define the structure of our data tables to ensure data types are correct

# Define schemas for all tables to match the expected structure
# This helps ensure our data types are correct when loading into Spark
SCHEMAS = {
    # CRM System Tables
    "cust_info": StructType([
        StructField("cst_id", IntegerType(), True),          # Customer ID (number)
        StructField("cst_key", StringType(), True),          # Customer Key (text)
        StructField("cst_firstname", StringType(), True),    # First Name (text)
        StructField("cst_lastname", StringType(), True),     # Last Name (text)
        StructField("cst_marital_status", StringType(), True), # Marital Status (text)
        StructField("cst_gndr", StringType(), True),         # Gender (text)
        StructField("cst_create_date", DateType(), True)     # Creation Date (date)
    ]),
    
    "prd_info": StructType([
        StructField("prd_id", IntegerType(), True),          # Product ID (number)
        StructField("prd_key", StringType(), True),          # Product Key (text)
        StructField("prd_nm", StringType(), True),           # Product Name (text)
        StructField("prd_cost", IntegerType(), True),        # Product Cost (number)
        StructField("prd_line", StringType(), True),         # Product Line (text)
        StructField("prd_start_dt", DateType(), True),       # Start Date (date)
        StructField("prd_end_dt", DateType(), True)          # End Date (date)
    ]),
    
    "sales_details": StructType([
        StructField("sls_ord_num", StringType(), True),      # Order Number (text)
        StructField("sls_prd_key", StringType(), True),      # Product Key (text)
        StructField("sls_cust_id", IntegerType(), True),     # Customer ID (number)
        StructField("sls_order_dt", DateType(), True),       # Order Date (date) - Fixed from IntegerType
        StructField("sls_ship_dt", DateType(), True),        # Ship Date (date) - Fixed from IntegerType
        StructField("sls_due_dt", DateType(), True),         # Due Date (date) - Fixed from IntegerType
        StructField("sls_sales", IntegerType(), True),       # Sales Amount (number)
        StructField("sls_quantity", IntegerType(), True),    # Quantity (number)
        StructField("sls_price", IntegerType(), True)        # Price (number)
    ]),
    
    # ERP System Tables
    "CUST_AZ12": StructType([
        StructField("cid", StringType(), True),              # Customer ID (text)
        StructField("bdate", DateType(), True),              # Birth Date (date)
        StructField("gen", StringType(), True)               # Gender (text)
    ]),
    
    "LOC_A101": StructType([
        StructField("cid", StringType(), True),              # Customer ID (text)
        StructField("cntry", StringType(), True)             # Country (text)
    ]),
    
    "PX_CAT_G1V2": StructType([
        StructField("id", StringType(), True),               # ID (text)
        StructField("cat", StringType(), True),              # Category (text)
        StructField("subcat", StringType(), True),           # Subcategory (text)
        StructField("maintenance", StringType(), True)       # Maintenance (text)
    ])
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# HELPER FUNCTIONS
# ============================================================
# These functions help us download and process the data

def fetch_data(url, max_retries=CONFIG["source"]["max_retries"], retry_delay=CONFIG["source"]["retry_delay"]):
    """
    Download data from a URL with retry logic if the download fails
    
    Args:
        url: The URL to download data from
        max_retries: Maximum number of retry attempts
        retry_delay: Seconds to wait between retries
    
    Returns:
        The text content of the downloaded file
    """
    # Try to download the file, with retries if it fails
    for attempt in range(max_retries):
        try:
            # Send a GET request to the URL
            logger.info(f"Downloading data from {url}")
            response = requests.get(url)
            
            # Check if the request was successful
            response.raise_for_status()
            
            # Return the content of the response
            return response.text
            
        except requests.exceptions.RequestException as e:
            # If we haven't used all our retry attempts, try again
            if attempt < max_retries - 1:
                logger.warning(f"Download attempt {attempt+1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                # If we've used all our retry attempts, log an error and raise the exception
                logger.error(f"All {max_retries} download attempts failed for {url}")
                raise

def preprocess_dataframe(pdf, file_name):
    """
    Clean and prepare the pandas DataFrame before converting to Spark
    
    Args:
        pdf: The pandas DataFrame to preprocess
        file_name: Name of the file being processed
    
    Returns:
        The preprocessed pandas DataFrame
    """
    logger.info(f"Preprocessing data for {file_name}")
    
    # Make all column names lowercase for consistency
    pdf.columns = [col.lower() for col in pdf.columns]
    
    # Apply specific preprocessing based on which file we're working with
    if file_name.lower() == "cust_info":
        # Convert customer ID to numeric
        pdf['cst_id'] = pd.to_numeric(pdf['cst_id'], errors='coerce')
        
        # Convert date columns to datetime
        if 'cst_create_date' in pdf.columns:
            pdf['cst_create_date'] = pd.to_datetime(pdf['cst_create_date'], errors='coerce')

    elif file_name.lower() == "prd_info":
        # Convert numeric columns
        pdf['prd_id'] = pd.to_numeric(pdf['prd_id'], errors='coerce')
        pdf['prd_cost'] = pd.to_numeric(pdf['prd_cost'], errors='coerce')
        
        # Convert date columns
        pdf['prd_start_dt'] = pd.to_datetime(pdf['prd_start_dt'], errors='coerce')
        pdf['prd_end_dt'] = pd.to_datetime(pdf['prd_end_dt'], errors='coerce')

    elif file_name.lower() == "sales_details":
        # Convert numeric columns
        pdf['sls_cust_id'] = pd.to_numeric(pdf['sls_cust_id'], errors='coerce')
        
        # Convert date columns - previously treated as integers, now properly as dates
        date_cols = ['sls_order_dt', 'sls_ship_dt', 'sls_due_dt']
        for col in date_cols:
            if col in pdf.columns:
                # First convert to numeric, then to datetime
                pdf[col] = pd.to_numeric(pdf[col], errors='coerce')
                # Convert integer date format (YYYYMMDD) to datetime
                pdf[col] = pd.to_datetime(pdf[col].astype(str), format='%Y%m%d', errors='coerce')
        
        # Convert other numeric columns
        numeric_cols = ['sls_sales', 'sls_quantity', 'sls_price']
        for col in numeric_cols:
            if col in pdf.columns:
                pdf[col] = pd.to_numeric(pdf[col], errors='coerce')

    elif file_name.upper() == "CUST_AZ12":
        # Convert date columns
        if 'bdate' in pdf.columns:
            pdf['bdate'] = pd.to_datetime(pdf['bdate'], errors='coerce')

    # Check for missing values and log them
    null_counts = pdf.isnull().sum()
    if null_counts.any():
        logger.info(f"Missing values found in {file_name}: {null_counts[null_counts > 0].to_dict()}")

    return pdf

def convert_to_spark_df(pdf, file_name):
    """
    Convert a pandas DataFrame to a Spark DataFrame with the correct schema
    
    Args:
        pdf: The pandas DataFrame to convert
        file_name: Name of the file being processed
    
    Returns:
        A Spark DataFrame
    """
    logger.info(f"Converting {file_name} to Spark DataFrame")
    
    # Use our predefined schema if we have one for this file
    if file_name in SCHEMAS:
        schema = SCHEMAS[file_name]
        logger.info(f"Using predefined schema for {file_name}")
        sdf = spark.createDataFrame(pdf, schema=schema)
    else:
        # If we don't have a schema, let Spark infer it
        logger.info(f"No schema defined for {file_name}, inferring schema")
        sdf = spark.createDataFrame(pdf)

    # Add metadata columns to track when and where this data came from
    sdf = sdf.withColumn("ingestion_date", current_timestamp())
    sdf = sdf.withColumn("source_file", lit(file_name))

    return sdf

def write_to_bronze(sdf, table_name):
    """
    Write a Spark DataFrame to a Delta table in the bronze layer
    
    Args:
        sdf: The Spark DataFrame to write
        table_name: Name of the target table
    
    Returns:
        Number of rows written to the table
    """
    logger.info(f"Writing data to bronze table: {table_name}")
    
    # Get the partition column from our configuration
    partition_col = CONFIG["target"]["partition_column"]

    # Write the data to a Delta table
    # - format("delta"): Use the Delta Lake format
    # - mode("overwrite"): Replace any existing data
    # - partitionBy(): Split the data into separate files based on the partition column
    sdf.write.format("delta") \
        .mode("overwrite") \
        .partitionBy(partition_col) \
        .saveAsTable(table_name)

    # Count how many rows we wrote and return the count
    count = spark.table(table_name).count()
    return count

def load_to_bronze(file_list, system):
    """
    Main function to load a list of files from a source system to the bronze layer
    
    Args:
        file_list: List of files to process
        system: Source system name (crm, erp)
    
    Returns:
        Dictionary with results of the processing
    """
    # Dictionary to store our results
    results = {}
    base_url = CONFIG["source"]["base_url"]

    # Process each file in the list
    for file in file_list:
        # Start timing how long this file takes to process
        start_time = time.time()
        
        # Build the URL and table name
        url = f"{base_url}/source_{system.lower()}/{file}.csv"
        table_name = f"{CONFIG['target']['database']}.{system.lower()}_{file.lower()}"

        logger.info(f"========== Processing: {file} ==========")
        logger.info(f"Source: {url}")
        logger.info(f"Target: {table_name}")

        try:
            # Step 1: Download the data
            content = fetch_data(url)
            
            # Check if the file is empty
            if not content.strip():
                logger.warning(f"File {file} is empty!")
                results[file] = {
                    "status": "warning",
                    "message": "Empty file",
                    "table": table_name
                }
                continue

            # Step 2: Load into pandas DataFrame
            pdf = pd.read_csv(StringIO(content))
            raw_count = len(pdf)
            logger.info(f"Downloaded {raw_count} rows from {url}")

            # Step 3: Clean and prepare the data
            pdf = preprocess_dataframe(pdf, file)

            # Step 4: Convert to Spark DataFrame
            sdf = convert_to_spark_df(pdf, file)

            # Step 5: Write to bronze table
            count = write_to_bronze(sdf, table_name)

            # Step 6: Log results and show sample data
            elapsed_time = time.time() - start_time
            logger.info(f"✅ Successfully loaded {count} rows into {table_name}")
            logger.info(f"Processing time: {elapsed_time:.2f} seconds")

            # Show a sample of the data we loaded
            sample_rows = CONFIG["target"]["sample_rows"]
            logger.info(f"Sample data from {table_name}:")
            spark.table(table_name).show(sample_rows, truncate=False)

            # Store the results
            results[file] = {
                "status": "success",
                "rows": count,
                "time": elapsed_time,
                "table": table_name
            }

        except Exception as e:
            # If anything goes wrong, log the error and continue with the next file
            logger.error(f"❌ Error processing {file}: {str(e)}")
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

# ============================================================
# MAIN EXECUTION
# ============================================================
# This is where we run the data loading process

# Set Spark configurations for better performance
if CONFIG["performance"]["use_arrow"]:
    logger.info("Enabling Apache Arrow for faster pandas-to-Spark conversion")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

# Track how long the entire process takes
logger.info("Starting bronze data load process")
overall_start_time = time.time()

# Dictionary to store all results
all_results = {}

# Process each source system
for system, files in CONFIG["source"]["systems"].items():
    logger.info(f"\n==== Loading {system.upper()} Data ====")
    # Load all files for this system
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

# Log a summary of what we did
overall_time = time.time() - overall_start_time
logger.info(f"\n=== Bronze Data Load Summary ===")
logger.info(f"Total execution time: {overall_time:.2f} seconds")
logger.info(f"Tables processed: {successful_tables}/{total_tables}")
logger.info(f"Total rows loaded: {total_rows}")

# Log overall success or failure
if successful_tables == total_tables:
    logger.info("✅ All bronze data loaded successfully")
else:
    logger.warning(f"⚠️ {total_tables - successful_tables} tables failed to load")
    # List the tables that failed
    failed_tables = []
    for system, system_results in all_results.items():
        for file, result in system_results.items():
            if result["status"] != "success":
                failed_tables.append(f"{system}.{file}: {result.get('error', 'Unknown error')}")
    
    logger.warning("Failed tables:")
    for table in failed_tables:
        logger.warning(f"  - {table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
