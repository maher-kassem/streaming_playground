from pyspark.sql import SparkSession
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
