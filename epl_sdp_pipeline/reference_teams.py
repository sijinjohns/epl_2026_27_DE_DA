from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    name = "epl_2026_27.gold.reference_teams",
    comment = "reference table for teams (to join teams and players table)"
)

def reference_teams():
    files = dbutils.fs.ls("/Volumes/epl_2026_27/source_system/epl_raw/players/")

    latest_file = max(
        [f for f in files if f.name.startswith("players_") and f.name.endswith(".json")],
        key=lambda f: f.name
    )

    df = spark.read.json(latest_file.path)

    df = df.select(
    F.explode("teams").alias("teams")
    )

    df = df.select("teams.*")

    df_teams = spark.read.table("epl_2026_27.gold.gold_teams")

    df_join = df.join(df_teams, df["short_name"] == df_teams["team_short_name"], "outer").select(
        df["id"],
        df["name"],
        df["short_name"],
        df_teams["team_id"],
        df_teams["team_name"],
        df_teams["team_short_name"]
    )

        # Split into matched and unmatched records
    df_matched = df_join.filter(F.col("id").isNotNull() & F.col("team_id").isNotNull())

    # Unmatched from JSON side (id present, team_id null)
    df_left_only = df_join.filter(F.col("id").isNotNull() & F.col("team_id").isNull()).alias("a")

    # Unmatched from gold_teams side (id null, team_id present)
    df_right_only = df_join.filter(F.col("id").isNull() & F.col("team_id").isNotNull()).alias("b")

    # Fuzzy match on first 3 letters of name vs team_name
    df_fuzzy = df_left_only.join(
        df_right_only,
        F.upper(F.substring(F.col("a.name"), 1, 3)) == F.upper(F.substring(F.col("b.team_name"), 1, 3)),
        "inner"
    ).select(
        F.coalesce(F.col("a.id"), F.col("b.id")).alias("id"),
        F.coalesce(F.col("a.name"), F.col("b.name")).alias("name"),
        F.coalesce(F.col("a.short_name"), F.col("b.short_name")).alias("short_name"),
        F.coalesce(F.col("a.team_id"), F.col("b.team_id")).alias("team_id"),
        F.coalesce(F.col("a.team_name"), F.col("b.team_name")).alias("team_name"),
        F.coalesce(F.col("a.team_short_name"), F.col("b.team_short_name")).alias("team_short_name")
    )

    # Remaining unmatched (no fuzzy match found)
    df_left_remaining = df_left_only.join(
        df_right_only,
        F.upper(F.substring(F.col("a.name"), 1, 3)) == F.upper(F.substring(F.col("b.team_name"), 1, 3)),
        "left_anti"
    ).select("id", "name", "short_name", "team_id", "team_name", "team_short_name")

    df_right_remaining = df_right_only.join(
        df_left_only,
        F.upper(F.substring(F.col("b.team_name"), 1, 3)) == F.upper(F.substring(F.col("a.name"), 1, 3)),
        "left_anti"
    ).select("id", "name", "short_name", "team_id", "team_name", "team_short_name")

    # Combine all: matched + fuzzy-merged + remaining unmatched
    df_final = df_matched.unionByName(df_fuzzy).unionByName(df_left_remaining).unionByName(df_right_remaining)

    return df_final


