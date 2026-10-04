from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.temporary_view(
    name = "silver_players_view"
)

def silver_players():
    df_silver = spark.readStream.table("epl_2026_27.silver.silver_players_staging")

    valid_condition = (
        F.col("players_id").isNotNull() &
        F.col("players_code").isNotNull()
    )

    df_silver = df_silver.filter(valid_condition)

    return df_silver

dp.create_streaming_table(
    name = "epl_2026_27.silver.silver_players",
    comment = "silver table for players"
)

dp.create_auto_cdc_flow(
    target = "epl_2026_27.silver.silver_players",
    source = "silver_players_view",
    keys = ["players_code"],
    sequence_by = "ingestion_date",
    stored_as_scd_type = 1
)