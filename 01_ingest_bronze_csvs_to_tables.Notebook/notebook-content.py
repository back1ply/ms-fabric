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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# Define explicit schemas for each table
# These schemas can be adjusted based on the actual data structure
schemas = {
    "bronze_crm_cust_info.csv": StructType([
        StructField("cst_id", StringType(), False),  # Primary key, not nullable
        StructField("cst_firstname", StringType(), True),
        StructField("cst_lastname", StringType(), True),
        StructField("cst_marital_status", StringType(), True),
        StructField("cst_gndr", StringType(), True),
        StructField("cst_create_date", DateType(), True)
    ]),
    
    "bronze_crm_prd_info.csv": StructType([
        StructField("prd_id", StringType(), False),  # Primary key
        StructField("prd_key", StringType(), True),
        StructField("prd_line", StringType(), True),
        StructField("prd_cost", DoubleType(), True),
        StructField("prd_start_dt", StringType(), True),
        StructField("prd_end_dt", StringType(), True)
    ]),
    
    # For other tables, we'll use schema inference with type hints
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define mapping between file names and target bronze tables
bronze_files = {
    "bronze_crm_cust_info.csv": "bronze.crm_cust_info",
    "bronze_crm_prd_info.csv": "bronze.crm_prd_info",
    "bronze_crm_sales_details.csv": "bronze.crm_sales_details",
    "bronze_erp_cust_az12.csv": "bronze.erp_cust_az12",
    "bronze_erp_loc_a101.csv": "bronze.erp_loc_a101",
    "bronze_erp_px_cat_g1v2.csv": "bronze.erp_px_cat_g1v2"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to infer schema from data with type hints
def infer_schema_with_hints(df):
    """
    Improves schema by inferring types from data patterns.
    This helps with consistent data types across ingestion runs.
    """
    # Sample data to infer types
    sample_df = df.limit(1000)
    
    # Examine columns for patterns
    inferred_df = df
    
    # Look for date patterns in string columns
    for col_name in df.columns:
        col_type = df.schema[col_name].dataType
        
        # Try to convert string columns that might be dates
        if isinstance(col_type, StringType):
            # Check if column name suggests it's a date
            if any(date_hint in col_name.lower() for date_hint in ['date', 'dt', 'day']):
                try:
                    # Try to convert to date
                    inferred_df = inferred_df.withColumn(
                        col_name,
                        F.to_date(F.col(col_name))
                    )
                    print(f"Inferred {col_name} as DateType")
                except:
                    pass
            
        # Try to convert numeric-looking strings to proper types
        if isinstance(col_type, StringType):
            # Check if values look like numbers
            numeric_check = sample_df.select(
                F.count(F.when(F.col(col_name).cast("double").isNotNull(), True)).alias("numeric_count"),
                F.count(F.col(col_name)).alias("total")
            ).collect()[0]
            
            # If >90% of non-null values can be cast to numeric, convert the column
            if numeric_check.total > 0 and numeric_check.numeric_count / numeric_check.total > 0.9:
                # Check if it's likely an integer or double
                try:
                    inferred_df = inferred_df.withColumn(
                        col_name,
                        F.col(col_name).cast("double")
                    )
                    print(f"Inferred {col_name} as DoubleType")
                except:
                    pass
    
    return inferred_df

# Loop through files and load into corresponding bronze tables
for file_name, table_name in bronze_files.items():
    print(f"\n=== Loading {file_name} into {table_name} ===")
    
    # Check if we have an explicit schema defined
    if file_name in schemas:
        print(f"Using explicit schema for {file_name}")
        # Read CSV with explicit schema
        df = spark.read.option("header", True) \
                      .schema(schemas[file_name]) \
                      .csv(f"Files/bronze/{file_name}")
    else:
        print(f"Using schema inference for {file_name}")
        # Read CSV with schema inference
        df = spark.read.option("header", True) \
                      .option("inferSchema", True) \
                      .csv(f"Files/bronze/{file_name}")
        
        # Apply additional type hints to improve schema
        df = infer_schema_with_hints(df)

    # Display inferred/explicit schema
    print("Schema:")
    df.printSchema()
    
    # Display sample data
    print("Sample data:")
    df.show(5, truncate=False)

    # Create table if not exists
    if not spark._jsparkSession.catalog().tableExists(table_name):
        print(f"Creating new table: {table_name}")
        # Save with schema
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        # Update existing table
        df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)  # Allow schema evolution

    print(f"✅ Successfully loaded into {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
