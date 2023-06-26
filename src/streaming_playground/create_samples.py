from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def sample_spark_df(spark: SparkSession) -> DeltaTable:
    booking_no = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    route_code = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
    valid_from_datetime = [
        "2020-01-01 00:00:00",
        "2020-01-02 00:00:00",
        "2020-01-03 00:00:00",
        "2020-01-04 00:00:00",
        "2020-01-05 00:00:00",
        "2020-01-06 00:00:00",
        "2020-01-07 00:00:00",
        "2020-01-08 00:00:00",
        "2020-01-09 00:00:00",
        "2020-01-10 00:00:00",
    ]

    data = zip(booking_no, route_code, valid_from_datetime)

    df = spark.createDataFrame(
        data, ["booking_no", "route_code", "valid_from_datetime"]
    )

    return df
