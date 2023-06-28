from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def delta_to_kafka(
    spark: SparkSession,
    source_path,
    topic_name: str,
    kafka_server: str,
    checkpoint_path: str,
):

    # Stream source to kafka
    stream_in = spark.readStream.format("delta").load(source_path)
    stream_in = stream_in.withColumn("value", F.to_json(F.struct(F.col("*"))))

    stream_out = (
        stream_in.writeStream.format("kafka")
        .trigger(once=True)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("kafka.bootstrap.servers", kafka_server)
        .option("topic", topic_name)
        .start()
    )

    stream_out.awaitTermination()
    return stream_out.lastProgress


def kafka_to_delta(
    spark: SparkSession,
    source_path,
    destination_path,
    checkpoint_path: str,
    topic_name: str,
    kafka_server: str,
):
    def _append_to_sink(df: DataFrame, batch_id: int):
        df = df.withColumn("_batch_id", F.lit(batch_id)).withColumn(
            "_batch_timestamp", F.current_timestamp()
        )
        df.write.option("mergeSchema", "true").format("delta").mode("append").save(
            destination_path
        )

    # Stream source to delta
    stream_in = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("fetchOffset.numRetries", "10")
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    stream_out = (
        stream_in.writeStream.foreachBatch(_append_to_sink)
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(source_path)
    )

    stream_out.awaitTermination()
    return stream_out.lastProgress
