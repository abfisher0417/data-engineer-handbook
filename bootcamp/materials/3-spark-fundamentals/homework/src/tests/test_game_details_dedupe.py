from chispa.dataframe_comparer import *
from ..jobs.game_details_dedupe import do_game_details_dedupe_transformation
from collections import namedtuple
GameDetails = namedtuple("GameDetails", "game_id, team_id, player_id, player_name")


def test_scd_generation(spark):
    source_data = [
        GameDetails(22200162, 1610612737, 1630249, "Vit Krejci"),
        GameDetails(22200162, 1610612737, 1630249, "Vit Krejci"),
        GameDetails(22200163, 1610612765, 202711, "Bojan Bogdanovic"),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_game_details_dedupe_transformation(spark, source_df)
    expected_data = [
        GameDetails(22200162, 1610612737, 1630249, "Vit Krejci"),
        GameDetails(22200163, 1610612765, 202711, "Bojan Bogdanovic"),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
