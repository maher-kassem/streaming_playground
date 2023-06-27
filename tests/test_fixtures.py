from pyspark.sql import SparkSession, DataFrame, functions as F



def test_fixture_spark_df_sample(spark_df_sample: DataFrame):
    assert spark_df_sample.count() == 10
    assert spark_df_sample.columns == ["booking_no", "route_code", "valid_from_datetime"]
    assert spark_df_sample.select(F.col("booking_no")).distinct().count() == 10
    assert spark_df_sample.select(F.col("route_code")).distinct().count() == 10
    assert spark_df_sample.select(F.col("valid_from_datetime")).distinct().count() == 10



def test_fixture_spark_df_two_extra_row(spark_df_two_extra_rows: DataFrame):
    assert spark_df_two_extra_rows.count() == 2
    assert spark_df_two_extra_rows.columns == ["booking_no", "route_code", "valid_from_datetime"]
    assert spark_df_two_extra_rows.select(F.col("booking_no")).distinct().count() == 2
    assert spark_df_two_extra_rows.select(F.col("route_code")).distinct().count() == 2
    assert spark_df_two_extra_rows.select(F.col("valid_from_datetime")).distinct().count() == 2


def test_fixture_spark(spark: SparkSession):
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"
    assert spark.conf.get("spark.sql.catalog.spark_catalog") == "org.apache.spark.sql.delta.catalog.DeltaCatalog"