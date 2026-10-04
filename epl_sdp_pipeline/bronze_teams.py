from pyspark.sql import functions as F
from pyspark import pipelines as dp

path = "/Volumes/epl_2026_27/source_system/epl_raw/teams/"

@dp.table(
    name = "epl_2026_27.bronze.bronze_teams",
    comment = "bronze table for teams"
)

def bronze_teams():
    df = (spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","json")\
        .option("multiLine","true")\
        .option("cloudFiles.inferColumnTypes","true")\
        .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
        .load(path)
    )

    df = df.select(
        F.explode("teams").alias("teams")
    )

    df = df.select("teams.*")

    df = df.select(
    "*",
    *[
        F.col(f"area.{field.name}").alias(f"area_{field.name}")
        for field in df.schema["area"].dataType.fields
    ],
    F.col("_metadata.file_name").alias("file_name"),
    F.current_timestamp().alias("ingestion_date")
    )

    df = df.drop(
    "area",
    "coach",
    "runningCompetitions",
    "squad",
    "staff"
    )

    return df