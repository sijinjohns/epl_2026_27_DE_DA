from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.temporary_view(
    name = "silver_teams_view"
)

def silver_teams():
    df_silver = spark.readStream.table("epl_2026_27.silver.silver_teams_staging")

    valid_condition = (
        F.col("id").isNotNull() &
        F.col("name").isNotNull()
    )

    df_silver = df_silver.filter(valid_condition)   

    return df_silver

dp.create_streaming_table(
    name = "epl_2026_27.silver.silver_teams",
    comment = "silver table for teams"
)

dp.create_auto_cdc_flow(
    target = "epl_2026_27.silver.silver_teams",
    source = "silver_teams_view",
    keys = ["id"],
    sequence_by = "lastUpdated",
    stored_as_scd_type = 1
)