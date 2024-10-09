# Databricks notebook source
import requests
import json
from datetime import datetime
import os
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# COMMAND ----------

# Initial Variables
endpoint = "https://api.openbrewerydb.org/breweries"
metadata_endpoint = "https://api.openbrewerydb.org/v1/breweries/meta"
bronze_path = '/datalake/bronze/breweries/'

# COMMAND ----------

# Retrieves the total number of rows available from the metadata endpoint
try:
    metadata_req = requests.get(metadata_endpoint)
    metadata = metadata_req.json()
    total_documents = int(metadata.get("total"))
    logging.info(f"Total Quantity of documents: {total_documents}")
except Exception as e:
    logging.error(f"Not possible to fetch metadata from {metadata_endpoint}. Error: {str(e)}")
    raise


# COMMAND ----------

def write_json_file(content: list, path: str, filename: str) -> None:
    '''
    Function responsible for writing a JSON file.

    content: content to be written to the file
    path: directory where the file will be written
    filename: name of the file

    '''
    try:
        # ensures the filename always has '.json' at the end
        filename = filename + '.json' if '.json' not in filename else filename

        today = datetime.now()
        year, month, day = str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)

        full_path = os.path.join(path, year, month, day)
        
        if not os.path.exists(full_path):
            os.makedirs(full_path)
            logging.info(f"Directory created: {full_path}")

        filepath = os.path.join(full_path, filename)

        json_content = json.dumps(content, indent=4)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(json_content)
        
        logging.info(f"File written successfully: {filepath}")

    except Exception as e:
        logging.warning(f'Error writing json file. Error: str(e)')

# COMMAND ----------

quantity = 0
current_page = 1

# Makes requests until the number of rows retrieved matches the one informed in the metadata request (variable total_documents).
while quantity < total_documents:
    try:
        page_endpoint = f"{endpoint}?page={current_page}&per_page=200"
        req  = requests.get(page_endpoint)
        if req.status_code == 200:
            content = req.json()

            write_json_file(content, bronze_path, f'brewery_list_{current_page}')

            current_page += 1
            quantity += len(content)

        else:
            raise Exception(f"Error requesting to {page_endpoint}. Status Code: {req.status_code}")
    except Exception as e:
        logging.error(f"Error trying to fetch the data from {page_endpoint}. Error: {str(e)}")
        raise

