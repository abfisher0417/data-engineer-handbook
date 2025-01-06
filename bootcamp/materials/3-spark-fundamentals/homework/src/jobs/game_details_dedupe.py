from pyspark.sql import SparkSession

query = """
WITH game_details_with_row_num AS (
  SELECT a.*,
  ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id, team_id, player_id) AS row_num
  FROM game_details AS a
)
SELECT game_id, team_id, player_id, player_name
FROM game_details_with_row_num
WHERE row_num = 1
"""


def do_game_details_dedupe_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("game_details_dedupe") \
        .getOrCreate()
    output_df = do_game_details_dedupe_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("game_details_deduped")
