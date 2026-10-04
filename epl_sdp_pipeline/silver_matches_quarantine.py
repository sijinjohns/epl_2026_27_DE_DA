from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name = "epl_2026_27.silver.silver_matches_quarantine",
    comment = "quarantine table for matches - records with issues"
)

def silver_matches_quarantine():
    df_silver_q = spark.readStream.table("epl_2026_27.silver.silver_matches_staging")

    valid_condition = (
        F.col("id").isNotNull() &
        F.col("homeTeam_id").isNotNull() &
        F.col("awayTeam_id").isNotNull() &
        F.col("lastUpdated").isNotNull() &
        (F.col("homeTeam_id") != F.col("awayTeam_id")) &
        (
            (F.col("status") != "FINISHED") |
            (
                F.col("score_fullTime_away").isNotNull() &
                F.col("score_fullTime_home").isNotNull() &
                F.col("score_winner").isNotNull()
            )
        )
    )

    df_silver_q = df_silver_q.filter(~valid_condition)

    return df_silver_q

