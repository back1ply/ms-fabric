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
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, to_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Define source files
crm_files = ["cust_info", "prd_info", "sales_details"]
erp_files = ["CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"]
base_url = "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets"

# Define schemas for tables that need explicit typing
schemas = {
    "cust_info": StructType([
        StructField("cst_id", StringType(), False),
        StructField("cst_firstname", StringType(), True),
        StructField("cst_lastname", StringType(), True),
        StructField("cst_marital_status", StringType(), True),
        StructField("cst_gndr", StringType(), True),
        StructField("cst_create_date", DateType(), True)
    ]),
    "sales_details": StructType([
        StructField("sls_ord_num", StringType(), False),
        StructField("sls_prd_key", StringType(), True),
        StructField("sls_cust_id", IntegerType(), True),  # Changed from StringType to IntegerType
        StructField("sls_order_dt", StringType(), True),
        StructField("sls_ship_dt", StringType(), True),
        StructField("sls_due_dt", StringType(), True),
        StructField("sls_sales", DoubleType(), True),
        StructField("sls_quantity", IntegerType(), True),
        StructField("sls_price", DoubleType(), True)
    ]),
    "CUST_AZ12": StructType([  # Added explicit schema for CUST_AZ12
        StructField("CID", StringType(), False),
        StructField("BDATE", DateType(), True),  # Using DateType for BDATE
        StructField("GEN", StringType(), True)
    ])
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to load data from GitHub and write directly to bronze tables
def load_to_bronze(file_list, system):
    for file in file_list:
        url = f"{base_url}/source_{system.lower()}/{file}.csv"
        table_name = f"bronze.{system.lower()}_{file.lower()}"
        
        print(f"Processing: {url} -> {table_name}")
        
        try:
            # Download the file
            response = requests.get(url)
            content = StringIO(response.text)
            
            # Read into pandas first to handle small files efficiently
            pdf = pd.read_csv(content)
            
            # Apply type conversions for specific files
            if file == "cust_info":
                # Ensure cst_id is treated as string
                pdf['cst_id'] = pdf['cst_id'].astype(str)
            elif file == "sales_details":
                # Ensure sls_cust_id is treated as integer
                pdf['sls_cust_id'] = pdf['sls_cust_id'].astype(int)
            elif file == "CUST_AZ12":
                # Convert BDATE to datetime if it exists
                if 'BDATE' in pdf.columns:
                    pdf['BDATE'] = pd.to_datetime(pdf['BDATE'], errors='coerce')
            
            # Convert to Spark DataFrame with schema if defined, otherwise infer
            if file in schemas:
                sdf = spark.createDataFrame(pdf, schema=schemas[file])
            else:
                sdf = spark.createDataFrame(pdf)
            
            # Write directly to bronze table
            sdf.write.format("delta").mode("overwrite").saveAsTable(table_name)
            
            # Show sample and count for verification
            count = spark.table(table_name).count()
            print(f"✅ Loaded {count} rows into {table_name}")
            print("Sample data:")
            spark.table(table_name).show(3, truncate=False)
            
        except Exception as e:
            print(f"❌ Error processing {url}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Disable Arrow optimization if needed
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# Load CRM data
print("\n== Loading CRM Data ==")
load_to_bronze(crm_files, "crm")

# Load ERP data
print("\n== Loading ERP Data ==")
load_to_bronze(erp_files, "erp")

print("\n✅ All bronze data loaded successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
