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
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define silver tables to validate
silver_tables = [
    {"name": "silver.crm_cust_info", "bronze": "bronze.crm_cust_info", "key": "cst_id"},
    {"name": "silver.crm_prd_info", "bronze": "bronze.crm_prd_info", "key": "prd_id"},
    {"name": "silver.crm_sales_details", "bronze": "bronze.crm_sales_details", "key": "sls_ord_num"},
    {"name": "silver.erp_cust_az12", "bronze": "bronze.erp_cust_az12", "key": "cid"},
    {"name": "silver.erp_loc_a101", "bronze": "bronze.erp_loc_a101", "key": "cid"},
    {"name": "silver.erp_px_cat_g1v2", "bronze": "bronze.erp_px_cat_g1v2", "key": "id"}
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate each silver table
for table in silver_tables:
    print(f"\n{'='*80}")
    print(f"VALIDATING {table['name']}")
    print(f"{'='*80}")
    
    # Load tables
    silver_df = spark.table(table['name'])
    bronze_df = spark.table(table['bronze'])
    
    # 1. Compare record counts
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    diff = bronze_count - silver_count
    diff_percent = (diff / bronze_count * 100) if bronze_count > 0 else 0
    
    print("Record Count Comparison:")
    print(f"  Bronze: {bronze_count} records")
    print(f"  Silver: {silver_count} records")
    print(f"  Difference: {diff} records ({diff_percent:.2f}%)")
    
    # 2. Check for nulls in key columns
    key_column = table['key']
    null_count = silver_df.filter(col(key_column).isNull()).count()
    
    print(f"\nNull Check for Primary Key ({key_column}):")
    if null_count > 0:
        print(f"  ❌ {null_count} null values found in primary key")
    else:
        print(f"  ✅ No null values in primary key")
    
    # 3. Check for duplicates in key column
    dup_count = silver_df.groupBy(key_column).count().filter("count > 1").count()
    
    print(f"\nDuplicate Check for Primary Key ({key_column}):")
    if dup_count > 0:
        print(f"  ❌ {dup_count} duplicate values found in primary key")
        # Show sample duplicates
        dups = silver_df.groupBy(key_column).count().filter("count > 1").limit(5)
        print("  Sample duplicates:")
        dups.show(truncate=False)
    else:
        print(f"  ✅ No duplicates in primary key")
    
    # 4. Check for date columns with future dates
    date_cols = [f.name for f in silver_df.schema.fields 
                if str(f.dataType).startswith("DateType") and f.name != "dwh_create_date"]
    
    if date_cols:
        print("\nDate Range Checks:")
        for date_col in date_cols:
            future_dates = silver_df.filter(col(date_col) > current_date()).count()
            if future_dates > 0:
                print(f"  ⚠️ {date_col}: {future_dates} future dates found")
            else:
                print(f"  ✅ {date_col}: No future dates")
    
    # 5. Show basic statistics for numeric columns
    numeric_cols = [f.name for f in silver_df.schema.fields 
                   if str(f.dataType).startswith("IntegerType") or 
                      str(f.dataType).startswith("DoubleType") or
                      str(f.dataType).startswith("LongType")]
    
    if numeric_cols:
        print("\nNumeric Column Statistics:")
        silver_df.select([min(col(c)).alias(f"min_{c}") for c in numeric_cols] +
                        [max(col(c)).alias(f"max_{c}") for c in numeric_cols] +
                        [avg(col(c)).alias(f"avg_{c}") for c in numeric_cols]).show()
    
    # 6. Show sample data
    print("\nSample Data:")
    silver_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"\n{'='*80}")
print("VALIDATION COMPLETED")
print(f"{'='*80}")
print(f"Validation completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
