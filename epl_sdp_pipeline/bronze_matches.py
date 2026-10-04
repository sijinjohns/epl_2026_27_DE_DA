from pyspark import pipelines as dp
from pyspark.sql import functions as F

path = "/Volumes/epl_2026_27/source_system/epl_raw/matches/"

@dp.table(
    name = "epl_2026_27.bronze.bronze_matches",
    comment = "bronze layer containing raw match data"
)
def bronze_matches():
    df = (spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","json")\
        .option("multiLine","true")\
        .option("cloudFiles.inferColumnTypes","true")\
        .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
        .load(path)\

    )
    
    df = df.select(
        F.explode("matches").alias("matches"),
        F.col("_metadata.file_name").alias("file_name"),
        F.current_timestamp().alias("ingestion_date")
    )
    
    df = df.select(
        "matches.*",
        "file_name",
        "ingestion_date"
        )


    return df