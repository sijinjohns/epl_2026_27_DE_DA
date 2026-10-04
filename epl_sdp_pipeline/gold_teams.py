from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    name = "epl_2026_27.gold.gold_teams",
    comment = "gold table for teams"
)

def gold_teams():
    df_gold = spark.read.table("epl_2026_27.silver.silver_teams")

    df_gold = df_gold.select(
        F.col("id").alias("team_id"),
        F.col("name").alias("team_name"),
        F.col("tla").alias("team_short_name"),
        F.col("venue").alias("stadium"),
        F.col("founded").alias("founded_year"),
        F.col("crest").alias("crest_url")
    )

    return df_gold