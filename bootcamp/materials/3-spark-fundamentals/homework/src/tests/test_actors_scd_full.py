from chispa.dataframe_comparer import *
from ..jobs.actors_scd_full_job import do_actor_scd_transformation
from collections import namedtuple

ActorYear = namedtuple("ActorYear", "actor actorid year quality_class is_active")
ActorScd = namedtuple("ActorScd", "actor actorid start_year end_year quality_class is_active")


def test_scd_generation(spark):
    source_data = [
        ActorYear("Meat Loaf", 123, 2018, 'Good', True),
        ActorYear("Meat Loaf", 123, 2019, 'Good', True),
        ActorYear("Meat Loaf", 123, 2020, 'Bad', True),
        ActorYear("Meat Loaf", 123, 2021, 'Bad', True),
        ActorYear("Skid Markel", 456, 2020, 'Bad', True),
        ActorYear("Skid Markel", 456, 2021, 'Bad', True)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Meat Loaf", 123, 2018, 2020, 'Good', True),
        ActorScd("Meat Loaf", 123, 2020, 9999, 'Bad', True),
        ActorScd("Skid Markel", 456, 2020, 9999, 'Bad', True)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
