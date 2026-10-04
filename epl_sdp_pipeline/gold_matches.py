from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    name = "epl_2026_27.gold.gold_matches",
    comment = "gold table for matches"
)

def gold_matches():
    df_gold_matches = spark.read.table("epl_2026_27.silver.silver_matches")

    df_gold_matches = df_gold_matches.drop("referees")

    df_gold_matches = df_gold_matches.select(
        F.col("id").alias("match_id"),
        F.col("utcDate").alias("match_date"),
        F.col("matchday").alias("matchday"),
        F.col("status").alias("match_status"),
        F.col("homeTeam_id").alias("home_team_id"),
        F.col("homeTeam_crest").alias("home_team_creat"),
        F.col("homeTeam_name").alias("home_team_name"),
        F.col("awayTeam_id").alias("away_team_id"),    
        F.col("awayTeam_crest").alias("away_team_crest"),
        F.col("awayTeam_name").alias("awat_team_name"),
        F.col("score_fullTime_home").alias("home_team_score"),
        F.col("score_fullTime_away").alias("away_team_score"),
        F.col("score_winner").alias("winner")
    )

    return df_gold_matches