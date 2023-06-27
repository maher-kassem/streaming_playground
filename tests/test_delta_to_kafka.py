from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import DataFrame, SparkSession

from streaming_playground.spark_delta_to_kafka import delta_to_kafka


def test_delta_to_kafka(
    spark: SparkSession, spark_df_sample: DataFrame, spark_df_two_extra_rows
):
    with TemporaryDirectory() as temp_dir:
        source_path = str(Path(temp_dir) / "source")
        checkpoint_path = str(Path(temp_dir) / "checkpoint")
        spark_df_sample.write.format("delta").mode("overwrite").save(source_path)

        _ = delta_to_kafka(
            spark,
            source_path,
            topic_name="test",
            kafka_server="localhost:31595",
            checkpoint_path=checkpoint_path,
        )
