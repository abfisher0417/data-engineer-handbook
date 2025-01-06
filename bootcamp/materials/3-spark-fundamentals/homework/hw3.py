from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, expr, col

# spark.stop()

spark = SparkSession.builder.appName("Homework3").master("spark://spark-iceberg:7077").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# Setting SPJ related configs
spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true') 
spark.conf.set('spark.sql.sources.v2.bucketing.pushPartValues.enabled','true')
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping','true')
spark.conf.set('spark.sql.requireAllClusterKeysForCoPartition','false')
spark.conf.set('spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled','true')

spark.sql("""CREATE DATABASE IF NOT EXISTS homework3""")

matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
matches = matches.withColumn("is_team_game", col("is_team_game").cast("boolean")) \
                 .withColumn("is_match_over", col("is_match_over").cast("boolean")) \
                 .withColumn("completion_date", col("completion_date").cast("timestamp")) \
                 .withColumn("match_duration", col("match_duration").cast("long"))

spark.sql("""DROP TABLE IF EXISTS homework3.matches""")
bucketedDDL = """
 CREATE TABLE IF NOT EXISTS homework3.matches (
     match_id STRING,
     mapid STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     game_variant_id STRING,
     is_match_over BOOLEAN,
     completion_date TIMESTAMP,
     match_duration LONG,
     game_mode STRING,
     map_variant_id STRING
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketedDDL)

matches.writeTo("homework3.matches").overwritePartitions()

match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
match_details = match_details.withColumn("previous_spartan_rank", col("previous_spartan_rank").cast("long")) \
                             .withColumn("spartan_rank", col("spartan_rank").cast("long")) \
                             .withColumn("previous_total_xp", col("previous_total_xp").cast("long")) \
                             .withColumn("total_xp", col("total_xp").cast("long")) \
                             .withColumn("previous_csr_tier", col("previous_csr_tier").cast("long")) \
                             .withColumn("previous_csr_designation", col("previous_csr_designation").cast("long")) \
                             .withColumn("previous_csr", col("previous_csr").cast("long")) \
                             .withColumn("previous_csr_percent_to_next_tier", col("previous_csr_percent_to_next_tier").cast("long")) \
                             .withColumn("previous_csr_rank", col("previous_csr_rank").cast("long")) \
                             .withColumn("current_csr_tier", col("current_csr_tier").cast("long")) \
                             .withColumn("current_csr_designation", col("current_csr_designation").cast("long")) \
                             .withColumn("current_csr", col("current_csr").cast("long")) \
                             .withColumn("current_csr_percent_to_next_tier", col("current_csr_percent_to_next_tier").cast("long")) \
                             .withColumn("current_csr_rank", col("current_csr_rank").cast("long")) \
                             .withColumn("player_rank_on_team", col("player_rank_on_team").cast("long")) \
                             .withColumn("player_finished", col("player_finished").cast("boolean")) \
                             .withColumn("player_total_kills", col("player_total_kills").cast("long")) \
                             .withColumn("player_total_headshots", col("player_total_headshots").cast("long")) \
                             .withColumn("player_total_weapon_damage", col("player_total_weapon_damage").cast("double")) \
                             .withColumn("player_total_shots_landed", col("player_total_shots_landed").cast("long")) \
                             .withColumn("player_total_melee_kills", col("player_total_melee_kills").cast("long")) \
                             .withColumn("player_total_melee_damage", col("player_total_melee_damage").cast("long")) \
                             .withColumn("player_total_assassinations", col("player_total_assassinations").cast("long")) \
                             .withColumn("player_total_ground_pound_kills", col("player_total_ground_pound_kills").cast("long")) \
                             .withColumn("player_total_shoulder_bash_kills", col("player_total_shoulder_bash_kills").cast("long")) \
                             .withColumn("player_total_grenade_damage", col("player_total_grenade_damage").cast("long")) \
                             .withColumn("player_total_power_weapon_damage", col("player_total_power_weapon_damage").cast("long")) \
                             .withColumn("player_total_power_weapon_grabs", col("player_total_power_weapon_grabs").cast("long")) \
                             .withColumn("player_total_deaths", col("player_total_deaths").cast("long")) \
                             .withColumn("player_total_assists", col("player_total_assists").cast("long")) \
                             .withColumn("player_total_grenade_kills", col("player_total_grenade_kills").cast("long")) \
                             .withColumn("did_win", col("did_win").cast("long")) \
                             .withColumn("team_id", col("team_id").cast("long"))

spark.sql("""DROP TABLE IF EXISTS homework3.match_details""")
bucketedDDL = """
 CREATE TABLE IF NOT EXISTS homework3.match_details (
    match_id STRING,
    player_gamertag STRING,
    previous_spartan_rank LONG,
    spartan_rank LONG,
    previous_total_xp LONG,
    total_xp LONG,
    previous_csr_tier LONG,
    previous_csr_designation LONG,
    previous_csr LONG,
    previous_csr_percent_to_next_tier LONG,
    previous_csr_rank LONG,
    current_csr_tier LONG,
    current_csr_designation LONG,
    current_csr LONG,
    current_csr_percent_to_next_tier LONG,
    current_csr_rank LONG,
    player_rank_on_team LONG,
    player_finished BOOLEAN,
    player_average_life STRING,
    player_total_kills  LONG,
    player_total_headshots LONG,
    player_total_weapon_damage DOUBLE,
    player_total_shots_landed LONG,
    player_total_melee_kills LONG,
    player_total_melee_damage LONG,
    player_total_assassinations LONG,
    player_total_ground_pound_kills LONG,
    player_total_shoulder_bash_kills LONG,
    player_total_grenade_damage LONG,
    player_total_power_weapon_damage LONG,
    player_total_power_weapon_grabs LONG,
    player_total_deaths LONG,
    player_total_assists LONG,
    player_total_grenade_kills LONG,
    did_win LONG,
    team_id LONG
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketedDDL)

match_details.writeTo("homework3.match_details").overwritePartitions()

medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals_matches_players = medals_matches_players.withColumn("count", col("count").cast("long"))

spark.sql("""DROP TABLE IF EXISTS homework3.medals_matches_players""")
bucketedDDL = """
 CREATE TABLE IF NOT EXISTS homework3.medals_matches_players (
     match_id STRING,
     player_gamertag STRING,
     medal_id STRING,
     count LONG
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketedDDL)

medals_matches_players.writeTo("homework3.medals_matches_players").overwritePartitions()

medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv").select("medal_id", "name")

maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv").select("mapid", "name")

medals_matches_players = spark.read.table("homework3.medals_matches_players")
match_details = spark.read.table("homework3.match_details")
matches = spark.read.table("homework3.matches")

aggregated_df = (
    medals_matches_players.alias("mmp")
    .join(
        matches.alias("m"),
        col("m.match_id") == col("mmp.match_id"),
        "left"
    )
    .join(
        match_details.alias("md"),
        (col("md.match_id") == col("mmp.match_id")) & (col("md.player_gamertag") == col("mmp.player_gamertag")),
        "left"
    )
    .join(
        broadcast(medals.alias("medals")),
        (col("medals.medal_id") == col("mmp.medal_id")),
        "left"
    )
    .join(
        broadcast(maps.alias("maps")),
        (col("maps.mapid") == col("m.mapid")),
        "left"
    )
)

projected_cols = aggregated_df.select(
    col("m.match_id"),
    col("md.player_gamertag"),
    col("maps.mapid"),
    col("maps.name").alias("map_name"),
    col("medals.medal_id"),
    col("medals.name").alias("medal_name"),
    col("md.player_total_kills"),
    col("m.playlist_id"),
    col("mmp.count").alias("medals_count")
)

projected_cols.createOrReplaceTempView("aggregated_data")

bucketedDDL = """
 CREATE TABLE IF NOT EXISTS homework3.matches_aggregated (
    match_id STRING,
    player_gamertag STRING,
    mapid STRING,
    map_name STRING,
    medal_id STRING,
    medal_name STRING,
    player_total_kills LONG,
    playlist_id STRING,
    medals_count STRING
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketedDDL)

spark.sql("INSERT INTO homework3.matches_aggregated SELECT * FROM aggregated_data")

spark.sql("""
WITH distinct_matches AS (
  SELECT DISTINCT match_id, player_gamertag, player_total_kills
  FROM homework3.matches_aggregated
)
SELECT player_gamertag, AVG(player_total_kills)
FROM distinct_matches
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1
""").show(truncate=False)

spark.sql("""
WITH distinct_matches AS (
  SELECT DISTINCT match_id, playlist_id
  FROM homework3.matches_aggregated
)
SELECT playlist_id, COUNT(1)
FROM distinct_matches
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1
""").show(truncate=False)

spark.sql("""
WITH distinct_matches AS (
  SELECT DISTINCT match_id, mapid, map_name
  FROM homework3.matches_aggregated
)
SELECT mapid, map_name, COUNT(1)
FROM distinct_matches
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 1
""").show(truncate=False)

spark.sql("""
SELECT mapid, map_name, count(medals_count)
FROM homework3.matches_aggregated
WHERE medal_name='Killing Spree'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 1
""").show(truncate=False)
