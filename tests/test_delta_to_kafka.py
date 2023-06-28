from pathlib import Path
from tempfile import TemporaryDirectory

from kafka.admin import KafkaAdminClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming_playground.spark_delta_vs_kafka import delta_to_kafka, kafka_to_delta

KAFKA_SERVER = "localhost:31595"
admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_SERVER])


def delete_topics(topic_names):
    try:
        admin_client.delete_topics(topics=topic_names)
    except Exception as e:
        print(e)


def test_delta_to_kafka(
    spark: SparkSession, spark_df_sample: DataFrame, topic_name="test"
):
    # Delete topic to remove if from a previous run
    delete_topics([topic_name])

    with TemporaryDirectory() as temp_dir:
        source_path = str(Path(temp_dir) / "source")
        checkpoint_path = str(Path(temp_dir) / "checkpoint")
        spark_df_sample.write.format("delta").mode("overwrite").save(source_path)

        _ = delta_to_kafka(
            spark,
            source_path,
            topic_name=topic_name,
            kafka_server=KAFKA_SERVER,
            checkpoint_path=checkpoint_path,
        )


def test_kafka_to_delta(
    spark: SparkSession, spark_df_sample: DataFrame, topic_name="test2"
):
    # Delete topic to remove if from a previous run
    delete_topics([topic_name])

    with TemporaryDirectory() as temp_dir:
        source_path = str(Path(temp_dir) / "source")
        destination_path = str(Path(temp_dir) / "destination")
        checkpoint_path_read = str(Path(temp_dir) / "checkpoint_write")
        checkpoint_path_write = str(Path(temp_dir) / "checkpoint_read")

        spark_df_sample.write.format("delta").mode("overwrite").save(source_path)
        schema = spark_df_sample.schema

        _ = delta_to_kafka(
            spark,
            source_path,
            topic_name=topic_name,
            kafka_server=KAFKA_SERVER,
            checkpoint_path=checkpoint_path_read,
        )

        _ = kafka_to_delta(
            spark,
            source_path,
            destination_path,
            checkpoint_path=checkpoint_path_write,
            topic_name=topic_name,
            kafka_server=KAFKA_SERVER,
        )

        df_destination = spark.read.format("delta").load(destination_path)
        df_destination = df_destination.select(
            F.from_json(F.decode("value", "iso-8859-1"), schema).alias("value")
        ).select("value.*")

        assert df_destination.count() == 10
