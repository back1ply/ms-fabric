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
# META       "default_lakehouse_workspace_id": "8d3001ae-2f0d-4d23-a9ee-f2698798f695",
# META       "known_lakehouses": [
# META         {
# META           "id": "bc5c5ff7-dbfe-46f0-af4c-e5b298762e86"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# Configuration parameters for transformation
# These can be modified without changing the transformation logic
config = {
    # General settings
    "overwrite_mode": "overwrite",  # Options: "overwrite", "append", "merge"
    "add_metadata_columns": True,   # Whether to add metadata columns like dwh_create_date
    
    # Table-specific settings
    "tables": {
        "crm_cust_info": {
            "source_table": "bronze.crm_cust_info",
            "target_table": "silver.crm_cust_info",
            "primary_key": "cst_id",
            "deduplication_enabled": True,
            "null_handling": "filter"  # Options: "filter", "replace"
        },
        "crm_prd_info": {
            "source_table": "bronze.crm_prd_info",
            "target_table": "silver.crm_prd_info",
            "primary_key": "prd_id",
            "deduplication_enabled": False
        },
        "crm_sales_details": {
            "source_table": "bronze.crm_sales_details",
            "target_table": "silver.crm_sales_details",
            "primary_key": "sls_ord_num",
            "deduplication_enabled": False
        },
        "erp_cust_az12": {
            "source_table": "bronze.erp_cust_az12",
            "target_table": "silver.erp_cust_az12",
            "primary_key": "cid",
            "deduplication_enabled": False
        },
        "erp_loc_a101": {
            "source_table": "bronze.erp_loc_a101",
            "target_table": "silver.erp_loc_a101",
            "primary_key": "cid",
            "deduplication_enabled": False
        },
        "erp_px_cat_g1v2": {
            "source_table": "bronze.erp_px_cat_g1v2",
            "target_table": "silver.erp_px_cat_g1v2",
            "primary_key": "id",
            "deduplication_enabled": False
        }
    },
    
    # Mapping dictionaries for standardization
    "gender_mapping": {
        "F": "Female", 
        "FEMALE": "Female", 
        "M": "Male", 
        "MALE": "Male"
    },
    "marital_status_mapping": {
        "S": "Single",
        "M": "Married"
    },
    "product_line_mapping": {
        "M": "Mountain",
        "R": "Road",
        "S": "Other Sales",
        "T": "Touring"
    },
    "country_mapping": {
        "DE": "Germany",
        "US": "United States",
        "USA": "United States"
    }
}

# Helper functions for common transformation tasks
def add_metadata_columns(df):
    """Add standard metadata columns to the dataframe"""
    if config["add_metadata_columns"]:
        return df.withColumn("dwh_create_date", current_date())
    return df

def deduplicate_by_key(df, key_column, date_column=None):
    """
    Remove duplicate records keeping only the latest version
    
    Args:
        df: DataFrame to deduplicate
        key_column: Column to use as the primary key
        date_column: Optional date column to determine the latest record
    
    Returns:
        Deduplicated DataFrame
    """
    if date_column:
        # Keep the latest record based on date
        window_spec = Window.partitionBy(key_column).orderBy(col(date_column).desc())
    else:
        # If no date column, just use row_number (arbitrary but deterministic)
        window_spec = Window.partitionBy(key_column).orderBy(lit(1))
        
    return df.withColumn("flag_last", row_number().over(window_spec)) \
             .filter("flag_last = 1") \
             .drop("flag_last")

def standardize_column(df, column_name, mapping_dict, default_value="n/a"):
    """
    Standardize values in a column based on a mapping dictionary
    
    Args:
        df: DataFrame containing the column
        column_name: Name of the column to standardize
        mapping_dict: Dictionary mapping source values to target values
        default_value: Value to use for unmapped values
        
    Returns:
        DataFrame with standardized column
    """
    # Create a when clause for each mapping
    when_clause = None
    for source, target in mapping_dict.items():
        condition = upper(trim(col(column_name))) == source
        if when_clause is None:
            when_clause = when(condition, target)
        else:
            when_clause = when_clause.when(condition, target)
    
    # Add the otherwise clause
    when_clause = when_clause.otherwise(default_value)
    
    # Apply the transformation
    return df.withColumn(column_name, when_clause)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_crm_cust_info():
    """
    Transform CRM customer information from bronze to silver layer
    
    Business Rules:
    1. Filter out records with null customer IDs
    2. Deduplicate by customer ID, keeping only the latest record by creation date
    3. Standardize first and last names by trimming whitespace
    4. Standardize marital status codes to full text values
    5. Standardize gender codes to full text values
    6. Add metadata columns for data warehouse tracking
    """
    # Get table configuration
    table_config = config["tables"]["crm_cust_info"]
    
    # Step 1: Load source data
    print(f"Loading data from {table_config['source_table']}")
    df = spark.table(table_config["source_table"])
    
    # Step 2: Filter null keys if configured
    if table_config.get("null_handling") == "filter":
        print("Filtering records with null primary keys")
        df = df.where(f"{table_config['primary_key']} IS NOT NULL")
    
    # Step 3: Deduplicate if enabled
    if table_config.get("deduplication_enabled", False):
        print(f"Deduplicating by {table_config['primary_key']} using cst_create_date")
        df = deduplicate_by_key(df, table_config["primary_key"], "cst_create_date")
    
    # Step 4: Clean and standardize text fields
    print("Standardizing text fields")
    df = df.withColumn("cst_firstname", trim(col("cst_firstname"))) \
           .withColumn("cst_lastname", trim(col("cst_lastname")))
    
    # Step 5: Standardize categorical fields using mapping dictionaries
    print("Standardizing categorical fields")
    df = standardize_column(df, "cst_marital_status", config["marital_status_mapping"])
    df = standardize_column(df, "cst_gndr", config["gender_mapping"])
    
    # Step 6: Add metadata columns
    df = add_metadata_columns(df)
    
    # Step 7: Write to target table
    print(f"Writing to {table_config['target_table']}")
    df.write.format("delta") \
        .mode(config["overwrite_mode"]) \
        .saveAsTable(table_config["target_table"])
    
    print(f"✅ Completed transformation of {table_config['source_table']} to {table_config['target_table']}")
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_crm_prd_info():
    """
    Transform CRM product information from bronze to silver layer
    
    Business Rules:
    1. Extract category ID from product key (first 5 characters)
    2. Clean product key by removing category prefix
    3. Standardize product line codes to full text values
    4. Replace null product costs with 0
    5. Convert date strings to proper date format
    6. Add metadata columns for data warehouse tracking
    """
    # Get table configuration
    table_config = config["tables"]["crm_prd_info"]
    
    # Step 1: Load source data
    print(f"Loading data from {table_config['source_table']}")
    df = spark.table(table_config["source_table"])
    
    # Step 2: Extract category ID from product key
    print("Extracting category ID from product key")
    df = df.withColumn("cat_id", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_"))
    
    # Step 3: Clean product key by removing category prefix
    print("Cleaning product key")
    df = df.withColumn("prd_key", expr("substring(prd_key, 7)"))
    
    # Step 4: Standardize product line using mapping dictionary
    print("Standardizing product line")
    df = standardize_column(df, "prd_line", config["product_line_mapping"])
    
    # Step 5: Handle missing values
    print("Handling missing values")
    df = df.withColumn("prd_cost", coalesce(col("prd_cost"), lit(0)))
    
    # Step 6: Convert date strings to proper date format
    print("Converting date strings to date format")
    df = df.withColumn("prd_start_dt", to_date(col("prd_start_dt"))) \
           .withColumn("prd_end_dt", to_date(col("prd_end_dt")))
    
    # Step 7: Add metadata columns
    df = add_metadata_columns(df)
    
    # Step 8: Write to target table
    print(f"Writing to {table_config['target_table']}")
    df.write.format("delta") \
        .mode(config["overwrite_mode"]) \
        .saveAsTable(table_config["target_table"])
    
    print(f"✅ Completed transformation of {table_config['source_table']} to {table_config['target_table']}")
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_crm_sales_details():
    df = spark.table("bronze.crm_sales_details")

    result = df.withColumn("sls_order_dt", when((col("sls_order_dt") == 0) | (length(col("sls_order_dt")) != 8), None)
                                           .otherwise(to_date(col("sls_order_dt").cast("string"), "yyyyMMdd"))) \
        .withColumn("sls_ship_dt", when((col("sls_ship_dt") == 0) | (length(col("sls_ship_dt")) != 8), None)
                                          .otherwise(to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd"))) \
        .withColumn("sls_due_dt", when((col("sls_due_dt") == 0) | (length(col("sls_due_dt")) != 8), None)
                                         .otherwise(to_date(col("sls_due_dt").cast("string"), "yyyyMMdd"))) \
        .withColumn("sls_sales_calc", col("sls_quantity") * abs(col("sls_price"))) \
        .withColumn("sls_sales", when((col("sls_sales").isNull()) | (col("sls_sales") <= 0) | (col("sls_sales") != col("sls_sales_calc")),
                                       col("sls_sales_calc")).otherwise(col("sls_sales"))) \
        .withColumn("sls_price", when((col("sls_price").isNull()) | (col("sls_price") <= 0),
                                       when(col("sls_quantity") != 0, col("sls_sales") / col("sls_quantity"))).otherwise(col("sls_price"))) \
        .drop("sls_sales_calc") \
        .withColumn("dwh_create_date", current_date())

    result.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver.crm_sales_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_erp_cust_az12():
    df = spark.table("bronze.erp_cust_az12")

    result = df.withColumn("cid", when(col("cid").startswith("NAS"), substring(col("cid"), 4, 100)).otherwise(col("cid"))) \
        .withColumn("bdate", when(col("bdate") > current_date(), None).otherwise(col("bdate"))) \
        .withColumn("gen", when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
                          .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
                          .otherwise("n/a")) \
        .withColumn("dwh_create_date", current_date())

    result.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver.erp_cust_az12")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_erp_loc_a101():
    df = spark.table("bronze.erp_loc_a101")

    result = df.withColumn("cid", regexp_replace(col("cid"), "-", "")) \
        .withColumn("cntry", when(trim(upper(col("cntry"))) == "DE", "Germany")
                              .when(trim(upper(col("cntry"))).isin("US", "USA"), "United States")
                              .when((col("cntry").isNull()) | (trim(col("cntry")) == ""), "n/a")
                              .otherwise(trim(col("cntry")))) \
        .withColumn("dwh_create_date", current_date())

    result.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver.erp_loc_a101")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_erp_px_cat_g1v2():
    df = spark.table("bronze.erp_px_cat_g1v2")

    result = df.withColumn("cat", trim(col("cat"))) \
        .withColumn("subcat", trim(col("subcat"))) \
        .withColumn("maintenance", trim(col("maintenance"))) \
        .withColumn("dwh_create_date", current_date())

    result.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver.erp_px_cat_g1v2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transform_crm_cust_info()
transform_crm_prd_info()
transform_crm_sales_details()
transform_erp_cust_az12()
transform_erp_loc_a101()
transform_erp_px_cat_g1v2()

print("✔ All Bronze ➝ Silver transformations completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
