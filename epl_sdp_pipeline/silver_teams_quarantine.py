from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name = "epl_2026_27.silver.silver_teams_quarantine",
    comment = "quarantine table for teams - records with issues"
)

def silver_teams_quarantine():
    df_silver_q = spark.readStream.table("epl_2026_27.silver.silver_teams_staging")

    valid_condition = (
        F.col("id").isNotNull() &
        F.col("name").isNotNull()
    )

    df_silver_q = df_silver_q.filter(~valid_condition)

    return df_silver_q
