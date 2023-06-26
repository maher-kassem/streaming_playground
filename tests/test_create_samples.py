from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming_playground.create_samples import sample_spark_df


def test_create_sample_spark_df(spark: SparkSession):
    df = sample_spark_df(spark)

    assert df.count() == 10
    assert df.columns == ["booking_no", "route_code", "valid_from_datetime"]

    assert df.select(F.col("booking_no")).distinct().count() == 10
    assert df.select(F.col("route_code")).distinct().count() == 10
    assert df.select(F.col("valid_from_datetime")).distinct().count() == 10
