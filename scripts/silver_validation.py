import logging
from delta import *
import pyspark
import data_quality as dt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    builder = pyspark.sql.SparkSession.builder.appName("Silver Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
except Exception as e:
    logging.error(f"Error trying to set spark object. Error: {str(e)}")
    raise


key_columns = ["id"]


try:
    #Reads data from the silver layer
    df_silver = spark.read.format("delta").load('/datalake/silver/breweries/')
except Exception as e:
    logging.error(f"Error trying to read data from silver layer. Error {str(e)}")
    raise


def run_data_quality_checks(df):
    results = {
        "empty_df": dt.check_empty_df(df),
        "unique_rows": dt.check_only_unique_rows(df, key_columns)
    }
    
    if all(results.values()):
        logging.info("All data quality checks passed successfully.")
    else:
        logging.error("Data quality checks failed.")
        raise


run_data_quality_checks(df_silver)