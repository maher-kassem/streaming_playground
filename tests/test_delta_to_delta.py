from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming_playground.spark_delta_to_delta import delta_to_delta


def test_delta_to_delta(spark: SparkSession, spark_df_sample: DataFrame):
    with TemporaryDirectory() as temp_dir:
        source_path = str(Path(temp_dir) / "source")
        destination_path = str(Path(temp_dir) / "destination")
        checkpoint_path = str(Path(temp_dir) / "checkpoint")

        # Write source to disk
        spark_df_sample.write.format("delta").mode("overwrite").save(source_path)


        # Stream source to destination
        info = delta_to_delta(
            spark,
            source_path=source_path,
            destination_path=destination_path,
            checkpoint_filename=checkpoint_path,
        )

        # Read destimation
        df_destination = spark.read.format("delta").load(destination_path)
        df_destination
