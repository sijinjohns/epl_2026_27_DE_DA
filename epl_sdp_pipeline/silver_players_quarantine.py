from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name = "epl_2026_27.silver.silver_players_quarantine",
    comment = "quarantine table for players"
)

def silver_players_quarantine():
    df_silver_q = spark.readStream.table("epl_2026_27.silver.silver_players_staging")

    valid_condition = (
        F.col("players_id").isNotNull() &
        F.col("players_code").isNotNull()
    )

    df_silver_q = df_silver_q.filter(~valid_condition)

    return df_silver_q