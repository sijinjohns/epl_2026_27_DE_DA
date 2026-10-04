from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    name = "epl_2026_27.gold.date",
    comment = "Date dimension table from 2026-01-01 to 2027-12-31"
)

def date():
    df = spark.range(0, 730).select(
        F.date_add(F.lit("2026-01-01"), F.col("id").cast("int")).alias("date")
    )

    df = df.select(
        F.col("date"),
        F.month(F.col("date")).alias("month"),
        F.quarter(F.col("date")).alias("quarter"),
        F.year(F.col("date")).alias("year")
    )

    return df