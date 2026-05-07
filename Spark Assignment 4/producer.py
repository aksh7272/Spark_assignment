import datetime
import threading
import os
from decimal import *
from time import sleep
from uuid import uuid4, UUID


from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-xmzwx.europe-central2.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'IWLVE2DISNGFEFB4',
    'sasl.password': 'cfltfC3JrzFx5JLjRT3rOhFT7EMXHjyf+PmJdPAGQFA93Kn/Tjw76uvbAfT15fkQ'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-9kgdwy.europe-central2.gcp.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('47H6FIODKILJ6KI4', 'cfltUC+bN/vVnI2lVzpSLxUo35Kmur3vwC05qBqoYMe+IIC/gC4i9Mm4GSmGWYvw')
})


# Fetch the latest Avro schema for the value
subject_name = 'ads_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema from Registery---")
print(schema_str)
print("=====================")

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})


# Load the CSV data into a pandas DataFrame
df = pd.read_csv('D:/Professional/Data Engineer/spark/Spark_streaming_Solution_complete/ad_data.csv', dtype={"ad_id": str})
# df = df.fillna('null')

# Iterate over DataFrame rows and produce to Kafka
for index, row in enumerate(df.itertuples(index=False)):
    # Create a dictionary from the row values
    data_value = row._asdict()
    # Produce to Kafka
    producer.produce(
        topic='ads_data',
        key=str(index),
        value=data_value,
        on_delivery=delivery_report
    )
    producer.flush()
    #break

print("Data successfully published to Kafka")
