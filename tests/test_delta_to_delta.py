from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming_playground.spark_delta_to_delta import spark_delta_to_delta


def test_delta_to_delta(
    spark: SparkSession, spark_df_sample: DataFrame, spark_df_two_extra_rows: DataFrame
):
    with TemporaryDirectory() as temp_dir:
        source_path = str(Path(temp_dir) / "source")
        destination_path = str(Path(temp_dir) / "destination")
        checkpoint_path = str(Path(temp_dir) / "checkpoint")

        # Write source to disk
        spark_df_sample.write.format("delta").mode("overwrite").save(source_path)

        # Stream source to destination
        _ = spark_delta_to_delta(
            spark,
            source_path=source_path,
            destination_path=destination_path,
            checkpoint_filename=checkpoint_path,
        )

        # Read destimation
        df_destination = spark.read.format("delta").load(destination_path)

        assert df_destination.count() == 10
        assert len(glob(f"{destination_path}/*.parquet")) == 1

        # Append two extra rows to source
        spark_df_two_extra_rows.write.format("delta").mode("append").save(source_path)

        # Stream source to destination again
        _ = spark_delta_to_delta(
            spark,
            source_path=source_path,
            destination_path=destination_path,
            checkpoint_filename=checkpoint_path,
        )
        assert len(glob(f"{destination_path}/*.parquet")) == 2

        # Read destimation again
        df_destination = spark.read.format("delta").load(destination_path)
        assert df_destination.count() == 12
        assert df_destination.filter(F.col("_batch_id") == 1).count() == 2

        # Compact
        dt = DeltaTable.forPath(spark, destination_path)
        dt.optimize().executeCompaction()
        assert len(glob(f"{destination_path}/*.parquet")) == 3

        # Vacuum with zero retention, should keep only the compacted file
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        dt.vacuum(0)
        assert len(glob(f"{destination_path}/*.parquet")) == 1
