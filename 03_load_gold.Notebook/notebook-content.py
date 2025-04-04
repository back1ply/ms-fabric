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

def drop_gold_tables():
    tables = [
        "gold.dim_customer",
        "gold.dim_product",
        "gold.fct_sales"
    ]
    for table in tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"🗑️ Dropped existing table: {table}")
        except Exception as e:
            print(f"⚠️ Could not drop table {table}: {e}")

def save_gold_table(df_func, table_name):
    df = df_func()
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"✅ Loaded {table_name} with {df.count()} rows")

# Drop previous gold tables
drop_gold_tables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_dim_customer():
    crm = spark.table("silver.crm_cust_info")
    az = spark.table("silver.erp_cust_az12")
    loc = spark.table("silver.erp_loc_a101")

    df = crm.alias("ci")\
        .join(az.alias("ca"), col("ci.cst_key") == col("ca.cid"), "left")\
        .join(loc.alias("la"), col("ci.cst_key") == col("la.cid"), "left")\
        .select(
            col("ci.cst_id").alias("customer_id"),
            col("ci.cst_key").alias("customer_key"),
            col("ci.cst_firstname").alias("first_name"),
            col("ci.cst_lastname").alias("last_name"),
            when(col("ci.cst_gndr") != "n/a", col("ci.cst_gndr")).otherwise(col("ca.gen")).alias("gender"),
            col("ci.cst_marital_status").alias("marital_status"),
            col("ca.bdate").alias("birth_date"),
            col("la.cntry").alias("country"),
            col("ci.dwh_create_date").alias("create_date")
        )

    return df.withColumn("customer_key", row_number().over(Window.orderBy("customer_id")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_dim_product():
    prod = spark.table("silver.crm_prd_info")
    cat = spark.table("silver.erp_px_cat_g1v2")

    df = prod.alias("p")\
        .join(cat.alias("c"), col("p.cat_id") == col("c.id"), "left")\
        .where(col("p.prd_end_dt").isNull())\
        .select(
            col("p.prd_id").alias("product_id"),
            col("p.prd_key").alias("product_key"),
            col("p.prd_nm").alias("product_name"),
            lit(None).alias("product_description"),
            col("p.prd_line").alias("product_line"),
            col("p.prd_cost").alias("product_cost"),
            lit(None).alias("product_category"),
            col("p.cat_id").alias("category_id"),
            col("c.cat").alias("category"),
            col("c.subcat").alias("subcategory"),
            col("c.maintenance").alias("maintenance")
        )

    return df.withColumn("product_key_sk", row_number().over(Window.orderBy("product_id"))).over(Window.orderBy("p.prd_start_dt", "p.prd_key")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_fct_sales():
    sales = spark.table("silver.crm_sales_details")
    cust = spark.table("gold.dim_customer")
    prod = spark.table("gold.dim_product")

    df = sales.alias("s")\
        .join(cust.alias("c"), col("s.cst_id") == col("c.customer_id"), "left")\
        .join(prod.alias("p"), col("s.prd_id") == col("p.product_id"), "left")\
        .select(
            col("s.sls_id").alias("sales_id"),
            col("p.product_key"),
            col("p.product_name"),
            col("c.first_name"),
            col("c.last_name"),
            col("c.country"),
            col("s.sls_order_dt").alias("order_date"),
            col("s.sls_quantity").cast("int").alias("quantity"),
            col("s.sls_price").cast("float").alias("price"),
            col("s.sls_sales").cast("float").alias("sales_amount"),
            col("s.sls_due_dt").alias("due_date"),
            col("s.sls_ship_dt").alias("shipping_date"),
            col("s.sls_order_num").alias("order_number")
        )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_gold_table(transform_dim_customer, "gold.dim_customer")
save_gold_table(transform_dim_product, "gold.dim_product")
save_gold_table(transform_fct_sales, "gold.fct_sales")

print("✅ All Gold transformations completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
