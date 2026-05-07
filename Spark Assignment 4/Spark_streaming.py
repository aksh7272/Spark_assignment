from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *

packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.spark:spark-avro_2.12:3.5.0",
    "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5"
]

spark = (
    SparkSession.builder
    .appName("Ad stream")
    .config("spark.jars.packages", ",".join(packages))
    .getOrCreate()
)

# Kafka Properties
kafka_config = {
    'bootstrap.servers': 'pkc-xmzwx.europe-central2.gcp.confluent.cloud:9092',
    'sasl.username': 'IWLVE2DISNGFEFB4',
    'sasl.password': 'cfltfC3JrzFx5JLjRT3rOhFT7EMXHjyf+PmJdPAGQFA93Kn/Tjw76uvbAfT15fkQ'
}

topic = "ads_data"

# Schema for Data
schema_str = """
{
  "type": "record",
  "name": "AdMetrics",
  "fields": [
    {"name": "ad_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "clicks", "type": "int"},
    {"name": "views", "type": "int"},
    {"name": "cost", "type": "double"}
  ]
}
"""


# Read Kafka stream and define datarame
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap.servers"])
    .option("subscribe", "ads_data")
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{kafka_config["sasl.username"]}" '
        f'password="{kafka_config["sasl.password"]}";'
    )
    .load()
)

df.printSchema()


# Kafka Binary Message convert in value column from avro Deserialization
# where struct column alias as ctr (Selecting all column)
data = (
    df.select(
        from_avro(col("value"), schema_str).alias("ctr")
    )
    .select("ctr.*")
)

# Write data into GCS bucket
query = (
    data.writeStream
    .format("parquet")
    .option("path", "gs://ak_sparkbucket/ad-data/")
    .option("checkpointLocation", "gs://ak_sparkbucket/checkpoints/ad-data/")
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()