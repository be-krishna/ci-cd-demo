# Databricks notebook source
# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/',recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists datamesh_bronze;
# MAGIC use schema datamesh_bronze;

# COMMAND ----------

# MAGIC %fs ls /FileStore/Iowa_liquor_sales

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

iowa_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/Iowa_liquor_sales/Iowa_liquor_sales.csv")
    .withColumn('City',upper(col('City')))
    .withColumn('Store_Name', upper(col('Store Name')))
    .withColumn('Address', upper(col('Address')))
    .withColumn('Category_Name', upper(col('Category Name')))
    .withColumn('Vendor_Name', upper(col('Vendor Name')))
    .withColumn('Item_Description', upper(col('Item Description')))
    .withColumn('Date',regexp_replace(col('Date'),'/','-'))
    .withColumn("StoreLocation_Lat",regexp_extract(col("Store Location"), r"([-0-9.]+) ([-0-9.]+)", 1).cast("double"))
    .withColumn("StoreLocation_Long",regexp_extract(col("Store Location"), r"([-0-9.]+) ([-0-9.]+)", 2).cast("double"))
    .select(col('Invoice/Item Number').alias('Invoice_Item_number'),
    'Date',
    col('Store Number').alias('Store_Number'),
    'Store_Name',
    'Address',
    'City',
    col('Zip Code').alias('Zip_Code'),
    'StoreLocation_Lat',
    'StoreLocation_Long',
    col('County Number').alias('County_Number'),
    'County',
    'Category',
    'Category_Name',
    col('Vendor Number').alias('Vendor_Number'),
    'Vendor_Name',
    col('Item Number').alias('Item_Number'),
    'Item_Description',
    'Pack',
    col('Bottle Volume (ml)').alias('Bottle_Volume_ml'),
    col('State Bottle Cost').alias('State_Bottle_Cost'),
    col('State Bottle Retail').alias('State_Bottle_Retail'),
    col('Bottles Sold').alias('Bottle_Sold'),
    col('Sale (Dollars)').alias('Sale_Dollars'),
    col('Volume Sold (Liters)').alias('Volume_Sold_Liters'),
    col('Volume Sold (Gallons)').alias('Volume_Sold_Gallons')
    )
    .write.mode('overwrite').saveAsTable('datamesh_bronze.iowa_facttable')
)

# COMMAND ----------

display(spark.read.table('datamesh_bronze.iowa_facttable'))

# COMMAND ----------

county_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/Iowa_liquor_sales/County.csv")
    .select('County','State','Population',col('Country/region').alias('Country_Region'))
    .write.mode("overwrite")
    .saveAsTable("datamesh_bronze.county")
)

# COMMAND ----------

display(spark.read.table('datamesh_bronze.county'))

# COMMAND ----------

df = (spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/Iowa_liquor_sales/Country.csv"))
display(df)

# COMMAND ----------



city_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/Iowa_liquor_sales/Country.csv")
    .withColumn("ct_City", upper(col("City")))
    .select(
      col('`S. No.`').alias('S_No'),
      "ct_City",
      col("Population").alias("ct_Population"),
      col("Country/region").alias("ct_Country"),
      col("State").alias("ct_State")
    )
    .write.mode('overwrite').saveAsTable('datamesh_bronze.city')
)
