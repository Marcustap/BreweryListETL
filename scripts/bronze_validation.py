import logging
from delta import *
import pyspark
from datetime import datetime
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


#Columns expected for brewery_list.json
columns_expected = ["id","name","brewery_type","address_2","address_3","city","postal_code","country",
                    "longitude","latitude","phone","website_url","state","street"]

bronze_path = '/datalake/bronze/breweries/'

today = datetime.now()
year, month, day = str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)

bronze_filepath = f"{bronze_path}{year}/{month}/{day}/*.json"

try:
    df = spark.read.json(bronze_filepath, multiLine=True)
except Exception as e:
    logging.error(f"Error reading raw data. Path: {bronze_filepath}. Error: {str(e)}")
    raise



def run_data_quality_checks(df):
    results = {
        "empty_df": dt.check_empty_df(df),
        "missing_columns": dt.check_missing_columns(columns_expected, df.columns)
    }
    if all(results.values()):
        logging.info("All data quality checks passed successfully.")
    else:
        logging.error("Data quality checks failed.")
        raise

run_data_quality_checks(df)

