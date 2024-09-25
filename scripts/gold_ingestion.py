# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from pyspark.sql.types import IntegerType
import pyspark
from delta import *
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# COMMAND ----------

builder = pyspark.sql.SparkSession.builder.appName("Gold Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# COMMAND ----------

try:
    #Reads data from the silver layer
    df_silver = spark.read.format("delta").load('/datalake/silver/breweries/')
except Exception as e:
    logging.error(f"Error trying to read data from silver layer. Error {str(e)}")
    raise

# COMMAND ----------

try:
    #Summmarize the data, with the quantity of breweries per type and state.
    df_agg_per_state = df_silver.groupBy(col("state"), col("brewery_type")).agg(count("*").alias("brewery_quantity")) \
        .withColumn("brewery_quantity", col("brewery_quantity").cast(IntegerType()))
except Exception as e:
    logging.error(f"Error trying to summarize the data. Error: {str(e)}")
    raise

def check_empty_df(df):
    if df.rdd.isEmpty():
        logging.warning("DataFrame está vazio!")
        raise ValueError("DataFrame da camada silver está vazio.")

def check_duplicates(df, subset_cols):
    duplicate_count = df.groupBy(subset_cols).count().filter("count > 1").count()
    if duplicate_count > 0:
        logging.error(f"Found duplicated rows in columns {subset_cols}.")
        raise ValueError("Found duplicated rows in Dataframe")


check_duplicates(df_agg_per_state, ["state", "brewery_type"])

check_empty_df(df_silver)


# COMMAND ----------
try:
    #Writes the data paritioned by state column
    df_agg_per_state.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("state") \
        .save('/datalake/gold/breweries/num_breweries_per_state')
    logging.info(f"Data written successfuly into gold layer")

except Exception as e:
    logging.error(f"Error trying to save the data in gold layer. Error: {str(e)}")
    raise

# COMMAND ----------


