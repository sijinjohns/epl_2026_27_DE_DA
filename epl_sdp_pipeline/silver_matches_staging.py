from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name = "epl_2026_27.silver.silver_matches_staging",
    comment = "Staging table for silver matches - contains flattened data"
)

def silver_matches_staging():
    df_silver = spark.readStream.table("epl_2026_27.bronze.bronze_matches")

    df_silver = df_silver.select(
        *[
            F.col(f"area.{field.name}").alias(f"area_{field.name}")
            for field in df_silver.schema["area"].dataType.fields 
        ],
        *[
            F.col(f"awayTeam.{field.name}").alias(f"awayTeam_{field.name}")
            for field in df_silver.schema["awayTeam"].dataType.fields 
        ],
        *[
            F.col(f"competition.{field.name}").alias(f"competition_{field.name}")
            for field in df_silver.schema["competition"].dataType.fields 
        ],
        *[
            F.col(f"homeTeam.{field.name}").alias(f"homeTeam_{field.name}")
            for field in df_silver.schema["homeTeam"].dataType.fields 
        ],
        *[
            F.col(f"score.{field.name}").alias(f"score_{field.name}")
            for field in df_silver.schema["score"].dataType.fields 
        ],
        *[
            F.col(f"odds.{field.name}").alias(f"odds_{field.name}")
            for field in df_silver.schema["odds"].dataType.fields 
        ],
        *[
            F.col(f"season.{field.name}").alias(f"season_{field.name}")
            for field in df_silver.schema["season"].dataType.fields 
        ],
        F.col("group"),
        F.col("id"),
        F.col("lastUpdated"),
        F.col("matchday"),
        F.col("referees"),
        F.col("stage"),
        F.col("status"),
        F.col("utcDate"),
        F.col("file_name"),
        F.col("ingestion_date")
        )
    
    df_silver = df_silver.select(
        "*",
        *[
            F.col(f"score_fullTime.{field.name}").alias(f"score_fullTime_{field.name}")
            for field in df_silver.schema["score_fullTime"].dataType.fields
        ],
        *[
            F.col(f"score_halfTime.{field.name}").alias(f"score_halfTime_{field.name}")
            for field in df_silver.schema["score_halfTime"].dataType.fields
        ]
        )

    df_silver = df_silver.drop(
    "score_fullTime",
    "score_halfTime"
    )

    return df_silver