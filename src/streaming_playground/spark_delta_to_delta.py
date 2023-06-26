from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def delta_to_delta(
    spark: SparkSession, source_path, destination_path, checkpoint_filename
):
    def _append_to_sink(df: DataFrame, batch_id: int):
        df = df.withColumn("_batch_id", F.lit(batch_id)).withColumn(
            "_batch_timestamp", F.current_timestamp()
        )
        df.write.option("mergeSchema", "true").format("delta").mode("append").save(
            destination_path
        )

    # Stream source to sink
    out_stream = spark.readStream.format("delta").load(source_path)

    out_stream = (
        out_stream.writeStream.foreachBatch(_append_to_sink)
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_filename)
        .outputMode("append")
        .start()
    )

    out_stream.awaitTermination()
    return out_stream.lastProgress
