from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name = "epl_2026_27.silver.silver_teams_staging",
    comment = "staging table for teams"
)

def silver_teams_staging():
    df_silver = spark.readStream.table("epl_2026_27.bronze.bronze_teams")

    return df_silver
