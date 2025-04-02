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
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# Configuration for validation
validation_config = {
    "tables": {
        "crm_cust_info": {
            "bronze_table": "bronze.crm_cust_info",
            "silver_table": "silver.crm_cust_info",
            "primary_key": "cst_id",
            "date_columns": ["cst_create_date", "dwh_create_date"],
            "categorical_columns": ["cst_marital_status", "cst_gndr"],
            "text_columns": ["cst_firstname", "cst_lastname"],
            "numeric_columns": []
        },
        "crm_prd_info": {
            "bronze_table": "bronze.crm_prd_info",
            "silver_table": "silver.crm_prd_info",
            "primary_key": "prd_id",
            "date_columns": ["prd_start_dt", "prd_end_dt", "dwh_create_date"],
            "categorical_columns": ["prd_line"],
            "text_columns": ["prd_key", "cat_id"],
            "numeric_columns": ["prd_cost"]
        },
        "crm_sales_details": {
            "bronze_table": "bronze.crm_sales_details",
            "silver_table": "silver.crm_sales_details",
            "primary_key": "sls_ord_num",
            "date_columns": ["sls_order_dt", "sls_ship_dt", "sls_due_dt", "dwh_create_date"],
            "categorical_columns": [],
            "text_columns": [],
            "numeric_columns": ["sls_quantity", "sls_price", "sls_sales"]
        },
        "erp_cust_az12": {
            "bronze_table": "bronze.erp_cust_az12",
            "silver_table": "silver.erp_cust_az12",
            "primary_key": "cid",
            "date_columns": ["bdate", "dwh_create_date"],
            "categorical_columns": ["gen"],
            "text_columns": ["name"],
            "numeric_columns": []
        },
        "erp_loc_a101": {
            "bronze_table": "bronze.erp_loc_a101",
            "silver_table": "silver.erp_loc_a101",
            "primary_key": "cid",
            "date_columns": ["dwh_create_date"],
            "categorical_columns": ["cntry"],
            "text_columns": ["city", "addr"],
            "numeric_columns": []
        },
        "erp_px_cat_g1v2": {
            "bronze_table": "bronze.erp_px_cat_g1v2",
            "silver_table": "silver.erp_px_cat_g1v2",
            "primary_key": "id",
            "date_columns": ["dwh_create_date"],
            "categorical_columns": ["cat", "subcat", "maintenance"],
            "text_columns": [],
            "numeric_columns": []
        }
    },
    "validation_rules": {
        "compare_record_counts": True,
        "check_nulls": True,
        "check_duplicates": True,
        "check_date_ranges": True,
        "check_categorical_values": True,
        "generate_data_profile": True
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def compare_record_counts(bronze_table, silver_table):
    """
    Compare record counts between bronze and silver tables
    
    Args:
        bronze_table: Name of the bronze table
        silver_table: Name of the silver table
    
    Returns:
        Dictionary with comparison results
    """
    bronze_count = spark.table(bronze_table).count()
    silver_count = spark.table(silver_table).count()
    diff = bronze_count - silver_count
    diff_percent = (diff / bronze_count * 100) if bronze_count > 0 else 0
    
    result = {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "difference": diff,
        "difference_percent": diff_percent,
        "status": "OK" if abs(diff_percent) < 5 else "WARNING" if abs(diff_percent) < 10 else "ERROR"
    }
    
    return result

def check_nulls(df, table_name):
    """
    Check for null values in each column of the dataframe
    
    Args:
        df: DataFrame to check
        table_name: Name of the table for reporting
    
    Returns:
        Dictionary with null check results
    """
    results = {}
    total_rows = df.count()
    
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            null_percent = (null_count / total_rows * 100)
            results[col_name] = {
                "null_count": null_count,
                "null_percent": null_percent,
                "status": "OK" if null_percent < 5 else "WARNING" if null_percent < 10 else "ERROR"
            }
    
    return results

def check_duplicates(df, id_column):
    """
    Check for duplicate values in the primary key column
    
    Args:
        df: DataFrame to check
        id_column: Primary key column name
    
    Returns:
        Dictionary with duplicate check results
    """
    dup_df = df.groupBy(id_column).count().filter("count > 1")
    dup_count = dup_df.count()
    
    if dup_count > 0:
        # Get sample of duplicates
        sample_dups = dup_df.limit(5).collect()
        sample_ids = [row[id_column] for row in sample_dups]
        
        return {
            "duplicate_count": dup_count,
            "status": "ERROR" if dup_count > 0 else "OK",
            "sample_duplicate_ids": sample_ids
        }
    else:
        return {
            "duplicate_count": 0,
            "status": "OK"
        }

def check_date_ranges(df, date_columns):
    """
    Check date columns for invalid values (future dates, etc.)
    
    Args:
        df: DataFrame to check
        date_columns: List of date column names
    
    Returns:
        Dictionary with date range check results
    """
    results = {}
    current_date = datetime.now().date()
    
    for date_col in date_columns:
        if date_col in df.columns:
            # Skip dwh_create_date as it's expected to be current
            if date_col == "dwh_create_date":
                continue
                
            # Check for future dates
            future_dates = df.filter(col(date_col) > current_date).count()
            
            # Check for very old dates (before 1950)
            old_dates = df.filter(col(date_col) < "1950-01-01").count()
            
            if future_dates > 0 or old_dates > 0:
                results[date_col] = {
                    "future_dates": future_dates,
                    "old_dates": old_dates,
                    "status": "WARNING" if future_dates > 0 or old_dates > 0 else "OK"
                }
    
    return results

def generate_data_profile(df, table_name, column_types):
    """
    Generate data profiling metrics for the table
    
    Args:
        df: DataFrame to profile
        table_name: Name of the table
        column_types: Dictionary with column types
    
    Returns:
        Dictionary with profiling results
    """
    profile = {
        "table_name": table_name,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "column_profiles": {}
    }
    
    # Profile numeric columns
    for col_name in column_types.get("numeric_columns", []):
        if col_name in df.columns:
            stats = df.select(
                min(col_name).alias("min"),
                max(col_name).alias("max"),
                avg(col_name).alias("mean"),
                expr(f"percentile({col_name}, 0.5)").alias("median"),
                stddev(col_name).alias("stddev")
            ).collect()[0]
            
            profile["column_profiles"][col_name] = {
                "type": "numeric",
                "min": stats["min"],
                "max": stats["max"],
                "mean": stats["mean"],
                "median": stats["median"],
                "stddev": stats["stddev"]
            }
    
    # Profile categorical columns
    for col_name in column_types.get("categorical_columns", []):
        if col_name in df.columns:
            # Get value distribution
            value_counts = df.groupBy(col_name).count().orderBy(desc("count")).limit(10)
            
            # Convert to dictionary
            value_dist = {row[col_name]: row["count"] for row in value_counts.collect() if row[col_name] is not None}
            
            # Get distinct count
            distinct_count = df.select(col_name).distinct().count()
            
            profile["column_profiles"][col_name] = {
                "type": "categorical",
                "distinct_values": distinct_count,
                "top_values": value_dist
            }
    
    return profile

def validate_table(table_config):
    """
    Comprehensive validation of a silver table
    
    Args:
        table_config: Configuration for the table validation
    
    Returns:
        Dictionary with validation results
    """
    table_name = table_config["silver_table"]
    bronze_table = table_config["bronze_table"]
    primary_key = table_config["primary_key"]
    
    print(f"\n🧪 Validating {table_name}...")
    
    # Load the silver table
    df = spark.table(table_name)
    
    validation_results = {
        "table_name": table_name,
        "validation_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "checks": {}
    }
    
    # 1. Compare record counts with bronze layer
    if validation_config["validation_rules"]["compare_record_counts"]:
        print("Comparing record counts with bronze layer...")
        count_results = compare_record_counts(bronze_table, table_name)
        validation_results["checks"]["record_counts"] = count_results
        
        print(f"  Bronze: {count_results['bronze_count']} records")
        print(f"  Silver: {count_results['silver_count']} records")
        print(f"  Difference: {count_results['difference']} records ({count_results['difference_percent']:.2f}%)")
        print(f"  Status: {count_results['status']}")
    
    # 2. Check for nulls
    if validation_config["validation_rules"]["check_nulls"]:
        print("\nChecking for null values...")
        null_results = check_nulls(df, table_name)
        validation_results["checks"]["nulls"] = null_results
        
        if null_results:
            for col_name, result in null_results.items():
                print(f"  {col_name}: {result['null_count']} nulls ({result['null_percent']:.2f}%) - {result['status']}")
        else:
            print("  No null values found")
    
    # 3. Check for duplicates
    if validation_config["validation_rules"]["check_duplicates"]:
        print("\nChecking for duplicates...")
        dup_results = check_duplicates(df, primary_key)
        validation_results["checks"]["duplicates"] = dup_results
        
        if dup_results["duplicate_count"] > 0:
            print(f"  {dup_results['duplicate_count']} duplicate {primary_key} values found - {dup_results['status']}")
            print(f"  Sample duplicates: {dup_results['sample_duplicate_ids']}")
        else:
            print("  No duplicates found")
    
    # 4. Check date ranges
    if validation_config["validation_rules"]["check_date_ranges"] and table_config.get("date_columns"):
        print("\nChecking date ranges...")
        date_results = check_date_ranges(df, table_config["date_columns"])
        validation_results["checks"]["date_ranges"] = date_results
        
        if date_results:
            for col_name, result in date_results.items():
                print(f"  {col_name}: {result['future_dates']} future dates, {result['old_dates']} very old dates - {result['status']}")
        else:
            print("  No date range issues found")
    
    # 5. Generate data profile
    if validation_config["validation_rules"]["generate_data_profile"]:
        print("\nGenerating data profile...")
        profile = generate_data_profile(df, table_name, {
            "numeric_columns": table_config.get("numeric_columns", []),
            "categorical_columns": table_config.get("categorical_columns", [])
        })
        validation_results["data_profile"] = profile
        
        # Print summary of profile
        print(f"  Row count: {profile['row_count']}")
        print(f"  Column count: {profile['column_count']}")
        
        # Print numeric column summaries
        for col_name, col_profile in profile.get("column_profiles", {}).items():
            if col_profile["type"] == "numeric":
                print(f"  {col_name} (numeric): min={col_profile['min']}, max={col_profile['max']}, mean={col_profile['mean']:.2f}")
            elif col_profile["type"] == "categorical":
                print(f"  {col_name} (categorical): {col_profile['distinct_values']} distinct values")
    
    return validation_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute validation for all tables
validation_results = {}

for table_name, table_config in validation_config["tables"].items():
    print(f"\n{'='*80}")
    print(f"VALIDATING {table_config['silver_table']}")
    print(f"{'='*80}")
    
    # Run comprehensive validation
    result = validate_table(table_config)
    validation_results[table_name] = result

# Print summary of all validations
print(f"\n{'='*80}")
print("VALIDATION SUMMARY")
print(f"{'='*80}")

for table_name, result in validation_results.items():
    table_config = validation_config["tables"][table_name]
    silver_table = table_config["silver_table"]
    
    # Get overall status
    status = "✅ PASSED"
    
    # Check record counts
    if "record_counts" in result["checks"]:
        if result["checks"]["record_counts"]["status"] == "ERROR":
            status = "❌ FAILED"
        elif result["checks"]["record_counts"]["status"] == "WARNING" and status == "✅ PASSED":
            status = "⚠️ WARNING"
    
    # Check duplicates
    if "duplicates" in result["checks"]:
        if result["checks"]["duplicates"]["status"] == "ERROR":
            status = "❌ FAILED"
    
    # Check nulls
    if "nulls" in result["checks"]:
        for col, null_result in result["checks"]["nulls"].items():
            if null_result["status"] == "ERROR":
                status = "❌ FAILED"
            elif null_result["status"] == "WARNING" and status == "✅ PASSED":
                status = "⚠️ WARNING"
    
    print(f"{silver_table}: {status}")

# Optional: Save validation results to a file for historical tracking
# This could be extended to write to a validation tracking table
validation_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
validation_summary = {
    "validation_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "results": validation_results
}

# Convert to JSON string for display
import json
print(f"\nValidation completed at {validation_summary['validation_time']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
