# Databricks notebook source
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, upper, udf
import re
import unicodedata
from delta import *
import pyspark
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# COMMAND ----------

def build_spark_context():
    try:
        builder = pyspark.sql.SparkSession.builder.appName("Silver Ingestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        logging.error(f"Error trying to set spark object. Error: {str(e)}")
        raise

# COMMAND ----------


def clean_string(input_str:str) -> str:
    '''
    Cleans malformed data
    '''
    if input_str is not None:
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        clean_str = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
        # Substitui caracteres não ASCII e caracteres especiais por '_'
        return re.sub(r'[^a-zA-Z0-9 ]', '_', clean_str)
    
    return input_str


if __name__ == '__main__':
    #Get Spark Context
    spark = build_spark_context()

    #Register UDF
    clean_string_udf = udf(clean_string, StringType())

    # COMMAND ----------

    bronze_path = '/datalake/bronze/breweries/'

    today = datetime.now()
    year, month, day = str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)

    bronze_filepath = f"{bronze_path}{year}/{month}/{day}/*.json"

    # COMMAND ----------

    try:
        df = spark.read.json(bronze_filepath, multiLine=True)
        logging.info(f"Number of rows in raw data: {df.count()}")
    except Exception as e:
        logging.error(f"Error reading raw data. Path: {bronze_filepath}. Error: {str(e)}")
        raise

    # COMMAND ----------

    # Cleans the 'state' column removing accents and replace non-ASCII characters
    try:
        clean_df = df.withColumn('state', clean_string_udf(col('state')))
    except Exception as e:
        logging.error(f"Error on UDF 'clean_string_udf'. Error: {str(e)}")
        raise

    # Removes rows with null values in important columns
    try:
        filtered_df = clean_df.filter(
            (col("state").isNotNull()) & (col("state") != '') &
            (col("id").isNotNull()) & (col("id") != '') &
            (col("brewery_type").isNotNull()) & (col("brewery_type") != '')
        )
    except Exception as e:
        logging.error(f"Error trying to remove Null values from the raw data. Error: {str(e)}")
        raise

    # COMMAND ----------

    #Transforms state values (partition by column) to uppercase
    normalized_state = filtered_df.withColumn("state", upper(col("state")))

    # COMMAND ----------

    try:
        final_df = normalized_state.select(
            "id",
            "name",
            "brewery_type",
            "street",
            "address_2",
            "address_3",
            "city",
            "postal_code",
            "country",
            "longitude",
            "latitude",
            "phone",
            "website_url",
            "state",
        )
    except Exception as e:
        logging.error(f"Error selecting columns to final_df. Error: {str(e)}")
        raise

    # COMMAND ----------

    #writes the result into the silver layer
    try:

        final_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("state") \
            .save('/datalake/silver/breweries/')
        logging.info("Data saved successfuly into silver layer")

    except Exception as e:
        logging.error(f"Error saving files in silver layer. Error: {str(e)}")
        raise

