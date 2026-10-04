# Databricks notebook source
players_folder = "/Volumes/epl_2026_27/source_system/epl_raw/players/"

# COMMAND ----------

import requests
from datetime import datetime, timezone

# COMMAND ----------

url = "https://fantasy.premierleague.com/api/bootstrap-static/"

# COMMAND ----------

ingestion_time = datetime.now(timezone.utc)

file_name = "None"
record_count = 0
error = "None"

try:
    response = requests.get(url)

    response.raise_for_status()

    data = response.json()

    if "elements"not in data:
        raise ValueError("Api response has no elements")
        error = "Api response has no elements"
    
    record_count = len(data["elements"])

    if record_count == 0:
        raise ValueError("Api response has elements")
        error = "Api response has no elements"

    status = "success"


except Exception as e:
    status = "failed"
    error = str(e)
    print("error :",error)

    metadata_players = {
    "source_system": "fantasy.premierleague",
    "file_name": file_name,
    "entity": "players",
    "end_point": url,
    "record_count": record_count,
    "ingestion_time": ingestion_time,
    "status": status,
    "error": error
     }
    
    df_metadata = spark.createDataFrame([metadata_players])

    df_metadata.write\
    .format("delta")\
    .mode("append")\
    .saveAsTable("epl_2026_27.audit.api_ingestion_log")

    raise


# COMMAND ----------

file_name = (
    f"players_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json"
)

file_path = f"{players_folder}/{file_name}"

# COMMAND ----------

import json

dbutils.fs.put(
    file_path,
    json.dumps(data),
    overwrite = False
)

# COMMAND ----------

metadata_players = {
    "source_system": "fantasy.premierleague",
    "file_name": file_name,
    "entity": "players",
    "end_point": url,
    "record_count": record_count,
    "ingestion_time": ingestion_time,
    "status": status,
    "error": error
}

# COMMAND ----------

df_metadata = spark.createDataFrame([metadata_players])

# COMMAND ----------

df_metadata.write\
    .format("delta")\
    .mode("append")\
    .saveAsTable("epl_2026_27.audit.api_ingestion_log")