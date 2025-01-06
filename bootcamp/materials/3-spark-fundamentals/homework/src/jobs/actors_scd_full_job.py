from pyspark.sql import SparkSession

query = """
WITH actors_with_lag AS (
	SELECT
		actor,
		actorid,
		year,
		LEAD(year, 1, '9999') OVER (PARTITION BY actorid ORDER BY year ASC) AS next_year,
		LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year ASC) AS previous_quality_class,
		quality_class,
		LAG(is_active) OVER (PARTITION BY actorid ORDER BY year ASC) AS previous_is_active,
		is_active
	FROM actors
),
with_change_indicator AS (
	SELECT *,
	CASE
		WHEN quality_class <> previous_quality_class THEN 1
		WHEN is_active <> previous_is_active THEN 1
		ELSE 0
	END AS change_indicator
	FROM actors_with_lag
),
with_streaks AS (
	SELECT *,
	SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY year) AS streak_identifier
	FROM with_change_indicator
)
SELECT
	actor,
	actorid,
	MIN(year) AS start_year,
	MAX(next_year) AS end_year,
	quality_class,
	is_active
FROM with_streaks
GROUP BY actor, actorid, streak_identifier, quality_class, is_active
"""


def do_actor_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd_full") \
        .getOrCreate()
    output_df = do_actor_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")
