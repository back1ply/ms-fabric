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
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

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

def trim_all_string_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df

def normalize_gender(col_):
    return when(upper(col(col_)).isin("F", "FEMALE"), "Female") \
           .when(upper(col(col_)).isin("M", "MALE"), "Male") \
           .otherwise("n/a")

def normalize_marital_status(col_):
    return when(upper(col(col_)) == "S", "Single") \
           .when(upper(col(col_)) == "M", "Married") \
           .otherwise("n/a")

def map_product_line(col_):
    return when(upper(col(col_)) == "M", "Mountain") \
           .when(upper(col(col_)) == "R", "Road") \
           .when(upper(col(col_)) == "S", "Other Sales") \
           .when(upper(col(col_)) == "T", "Touring") \
           .otherwise("n/a")

def normalize_country(col_):
    return when(trim(col(col_)).isNull() | (col(col_) == "") | (upper(col(col_)) == "NAN"), "n/a") \
           .when(upper(col(col_)) == "DE", "Germany") \
           .when(upper(col(col_)).isin("US", "USA"), "United States") \
           .otherwise(initcap(col(col_)))

def add_metadata(df):
    return df.withColumn("dwh_create_date", current_date())

SILVER_TABLES = {
    "silver.crm_cust_info": lambda: transform_crm_cust_info(),
    "silver.crm_prd_info": lambda: transform_crm_prd_info(),
    "silver.crm_sales_details": lambda: transform_crm_sales_details(),
    "silver.erp_cust_az12": lambda: transform_erp_cust_az12(),
    "silver.erp_loc_a101": lambda: transform_erp_loc_a101(),
    "silver.erp_px_cat_g1v2": lambda: transform_erp_px_cat_g1v2()
}

def drop_silver_tables():
    for table in SILVER_TABLES:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"🗑️ Dropped existing table: {table}")
        except Exception as e:
            print(f"⚠️ Could not drop table {table}: {e}")

def save_silver_table(df_func, table_name):
    df = df_func()
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"✅ Loaded {table_name} with {df.count()} rows")

# Drop all target tables first
drop_silver_tables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_crm_cust_info():
    df = spark.table("bronze.crm_cust_info")
    df = trim_all_string_columns(df)

    w = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())

    return add_metadata(
        df
        .filter(col("cst_id").isNotNull())
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("cst_gndr", normalize_gender("cst_gndr"))
        .withColumn("cst_marital_status", normalize_marital_status("cst_marital_status"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_crm_prd_info():
    df = spark.table("bronze.crm_prd_info")
    df = trim_all_string_columns(df)

    w = Window.partitionBy("prd_key").orderBy("prd_start_dt")

    return add_metadata(df
        .withColumn("cat_id", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_"))
        .withColumn("prd_key", expr("substring(prd_key, 7)"))
        .withColumn("prd_line", map_product_line("prd_line"))
        .withColumn("prd_start_dt", col("prd_start_dt").cast("date"))
        .withColumn(
            "prd_end_dt",
            (lead("prd_start_dt").over(w) - expr("INTERVAL 1 day")).cast("date")
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_crm_sales_details():
    df = spark.table("bronze.crm_sales_details")

    return add_metadata(
        df
        .withColumn("sls_quantity", col("sls_quantity").cast("double"))
        .withColumn("sls_price", col("sls_price").cast("double"))
        .withColumn("sls_sales", when(
            col("sls_sales").isNull() | (col("sls_sales") <= 0) |
            (col("sls_quantity") * abs(col("sls_price")) != col("sls_sales")),
            col("sls_quantity") * abs(col("sls_price"))
        ).otherwise(col("sls_sales")))
        .withColumn("sls_price", when(
            col("sls_price").isNull() | (col("sls_price") <= 0),
            col("sls_sales") / when(col("sls_quantity") == 0, None).otherwise(col("sls_quantity"))
        ).otherwise(col("sls_price")))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_erp_cust_az12():
    df = spark.table("bronze.erp_cust_az12")
    df = trim_all_string_columns(df)

    return add_metadata(
        df
        .withColumn("cid", when(col("cid").startswith("NAS"), substring(col("cid"), 4, 100)).otherwise(col("cid")))
        .withColumn("bdate", when(
            (col("bdate") > current_date()) | (col("bdate") < lit("1924-01-01").cast("date")),
            None
        ).otherwise(col("bdate")))
        .withColumn("gen", normalize_gender("gen"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_erp_loc_a101():
    df = spark.table("bronze.erp_loc_a101")
    df = trim_all_string_columns(df)
    return add_metadata(df
        .withColumn("cid", regexp_replace(col("cid"), "-", ""))
        .withColumn("cntry", normalize_country("cntry"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_erp_px_cat_g1v2():
    df = spark.table("bronze.erp_px_cat_g1v2")
    df = trim_all_string_columns(df)
    return add_metadata(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name, transform_func in SILVER_TABLES.items():
    save_silver_table(transform_func, table_name)

print("✅ All Silver transformations completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
