from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name = "epl_2026_27.silver.silver_players_staging",
    comment = "silver staging table for players"
)

def silver_bronze_players():
    df_silver = spark.readStream.table("epl_2026_27.bronze.bronze_players")

    df_silver = df_silver.drop(
    F.col("players_price_change_projections"),
    F.col("players_scout_risks")
    )

    return df_silver