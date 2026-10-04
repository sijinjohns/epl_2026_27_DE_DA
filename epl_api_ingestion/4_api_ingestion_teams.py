# Databricks notebook source
teams_folder = "/Volumes/epl_2026_27/source_system/epl_raw/teams/"

# COMMAND ----------

import requests
from datetime import datetime, timezone

# COMMAND ----------

api_token = dbutils.secrets.get(
    catalog = "epl_2026_27",
    schema = "source_system",
    key = "football_api_token"
)

# COMMAND ----------

url = "http://api.football-data.org/v4/competitions/PL/teams?season=2026"

# COMMAND ----------

ingestion_time = datetime.now(timezone.utc)

file_name = "None"
record_count = 0
error = "None"

try:
    headers = {
        "X-Auth-Token": api_token
    }

    response = requests.get(url, headers = headers, timeout = 30)
    
    response.raise_for_status()

    data = response.json()

    if "teams" not in data:
        raise ValueError("Api response has no teams key")
        error = "Api response has no teams key"
    
    record_count = len(data["teams"])

    if record_count == 0:
        raise ValueError("Api response has no teams")
        error = "Api response has no teams"

    status = "success"


except Exception as e:
    status = "failed"
    error = str(e)
    print("error :",error)

    metadata_teams = {
    "source_system": "football-data.org",
    "file_name": file_name,
    "entity": "teams",
    "end_point": url,
    "record_count": record_count,
    "ingestion_time": ingestion_time,
    "status": status,
    "error": error
     }
    
    df_metadata = spark.createDataFrame([metadata_teams])

    df_metadata.write\
    .format("delta")\
    .mode("append")\
    .saveAsTable("epl_2026_27.audit.api_ingestion_log")

    raise


# COMMAND ----------


file_name = (
    f"teams_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json"
)

file_path = f"{teams_folder}/{file_name}"

# COMMAND ----------


import json

dbutils.fs.put(
    file_path,
    json.dumps(data),
    overwrite = False
)

# COMMAND ----------

metadata_teams = {
    "source_system": "football-data.org",
    "file_name": file_name,
    "entity": "teams",
    "end_point": url,
    "record_count": record_count,
    "ingestion_time": ingestion_time,
    "status": status,
    "error": error
}

# COMMAND ----------

df_metadata = spark.createDataFrame([metadata_teams])

# COMMAND ----------

df_metadata.write\
    .format("delta")\
    .mode("append")\
    .saveAsTable("epl_2026_27.audit.api_ingestion_log")