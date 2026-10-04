from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.temporary_view(
    name = "silver_matches_view"
)

def silver_matches():
    df_silver = spark.readStream.table("epl_2026_27.silver.silver_matches_staging")

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

    df_silver = df_silver.filter(valid_condition)

    return df_silver

dp.create_streaming_table(
    name = "epl_2026_27.silver.silver_matches",
    comment = "silver table for matches"
)

dp.create_auto_cdc_flow(
    target = "epl_2026_27.silver.silver_matches",
    source = "silver_matches_view",
    keys = ["id"],
    sequence_by = "lastUpdated",
    stored_as_scd_type = 1
)

