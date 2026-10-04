from pyspark.sql import functions as F
from pyspark import pipelines as dp

path = "/Volumes/epl_2026_27/source_system/epl_raw/players/"

@dp.table(
    name = "epl_2026_27.bronze.bronze_players",
    comment = "bronze table for players"
)

def bronze_players():
    df = (spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format","json")\
        .option("multiline","true")\
        .option("cloudFiles.inferColumnTypes","true")\
        .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
        .load(path)
    )
    
    df = df.select(
        F.explode("elements").alias("players")
    )

    df = df.select(
        *[
            F.col(f"players.{field.name}").alias(f"players_{field.name}")
            for field in df.schema["players"].dataType.fields
        ],
        F.col("_metadata.file_name").alias("file_name"),
        F.current_timestamp().alias("ingestion_date")
    )

    return df

