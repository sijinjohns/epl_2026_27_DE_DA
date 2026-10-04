from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    name = "epl_2026_27.gold.gold_players",
    comment = "gold table for players"
)

def gold_players():
    df_gold_players = spark.read.table("epl_2026_27.silver.silver_players")

    df_gold_players = df_gold_players.select(
    F.col("players_id").alias("player_id"),
    F.col("players_first_name").alias("first_name"),
    F.col("players_second_name").alias("last_name"),
    F.col("players_web_name").alias("player_name"),
    F.col("players_birth_date").alias("player_birth_date"),
    F.col("players_team").alias("fpl_team_id"),
    F.col("players_element_type").alias("position_id"),
    F.col("players_minutes").alias("minutes_played"),
    F.col("players_starts").alias("starting_eleven"),
    F.col("players_goals_scored").alias("goals"),
    F.col("players_assists").alias("assists"),
    F.col("players_expected_goals").alias("expected_goals"),
    F.col("players_expected_assists").alias("expected_assists"),
    F.col("players_expected_goal_involvements").alias("expected_goal_involvements"),
    F.col("players_clean_sheets").alias("clean_sheets"),
    F.col("players_goals_conceded").alias("goals_conceded"),
    F.col("players_yellow_cards").alias("yellow_card"),
    F.col("players_red_cards").alias("red_card"),
    F.col("players_status").alias("status"),
    F.col("players_now_cost").alias("value"),
    F.col("players_total_points").alias("points"),
    F.col("players_points_per_game").alias('points_per_game')
    )

    df_gold_players = df_gold_players.withColumn(
    "status",
    F.when(F.col("status") == "a", "Available")
     .when(F.col("status") == "i", "Injured")
     .when(F.col("status") == "u", "Unavailable")
     .when(F.col("status") == "d", "Doubtful")
     .when(F.col("status") == "s", "Suspended")
     .when(F.col("status") == "n", "Not in squad")
     .otherwise("Unknown")
    )

    return df_gold_players