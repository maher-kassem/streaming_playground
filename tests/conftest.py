import pytest
from pyspark.sql import DataFrame, SparkSession

SCALA_VERSION = "2.12"
SPARK_VERSION = "3.4.1"

ARTIFACTS_SPARK_33 = [
    # "com.microsoft.azure:azure-data-lake-store-sdk:2.3.9",
    # "com.microsoft.azure:azure-storage:7.0.1",
    "org.apache.commons:commons-pool2:2.11.1",
    f"io.delta:delta-core_{SCALA_VERSION}:2.4.0",
    # "org.apache.hadoop:hadoop-azure:3.3.2",
    # "org.apache.hadoop:hadoop-azure-datalake:3.3.2",
    # "org.eclipse.jetty:jetty-util:9.4.43.v20210629",
    # "org.eclipse.jetty:jetty-util-ajax:9.4.43.v20210629",
    "org.apache.kafka:kafka-clients:2.8.1",
    # "com.microsoft.sqlserver:mssql-jdbc:11.2.1.jre11",
    f"org.apache.spark:spark-avro_{SCALA_VERSION}:{SPARK_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}",
    f"org.apache.spark:spark-token-provider-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}",
    # "org.wildfly.openssl:wildfly-openssl:2.2.5.Final",
    # "com.azure.cosmos.spark:azure-cosmos-spark_3-3_2-12:4.15.0",
]


@pytest.fixture(scope="session")
def spark():
    builder = SparkSession.builder.appName("test").master("local[1]")

    config_extra = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",  # enable delta capabilites for e.g. deltaTable and corresponding utilitiy functions
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }

    for key, value in config_extra.items():
        builder.config(key=key, value=value)

    packages = ",".join(ARTIFACTS_SPARK_33)
    builder = builder.config("spark.jars.packages", packages)

    return builder.getOrCreate()


def sample_spark_df(spark: SparkSession) -> DataFrame:
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


def two_extra_rows(spark: SparkSession) -> DataFrame:
    booking_no = [11, 12]
    route_code = ["J", "K"]
    valid_from_datetime = ["2020-01-11 00:00:00", "2020-01-12 00:00:00"]
    data = zip(booking_no, route_code, valid_from_datetime)

    extra_rows = spark.createDataFrame(
        data, ["booking_no", "route_code", "valid_from_datetime"]
    )

    return extra_rows


@pytest.fixture(scope="session")
def spark_df_sample(spark: SparkSession):
    return sample_spark_df(spark)


@pytest.fixture(scope="session")
def spark_df_two_extra_rows(spark: SparkSession):
    return two_extra_rows(spark)
