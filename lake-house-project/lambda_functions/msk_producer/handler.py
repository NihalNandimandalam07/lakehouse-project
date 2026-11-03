"""Publishes JSON messages to the lakehouse MSK topic, creating the topic if needed.

Invoke with:
    {"messages": [{"employee_id": 1, "employee_name": "...", "department": "...", "salary": 1}]}
"""

import json
import os

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.sasl.oauth import AbstractTokenProvider

REGION = os.environ["AWS_REGION"]
BOOTSTRAP_SERVERS = os.environ["BOOTSTRAP_SERVERS"].split(",")
TOPIC = os.environ["KAFKA_TOPIC"]


class MSKTokenProvider(AbstractTokenProvider):
    #signs an IAM auth token with the Lambda role's credentials
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token


#MSK IAM auth over TLS (port 9098)
CLIENT_CONFIG = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "OAUTHBEARER",
    "sasl_oauth_token_provider": MSKTokenProvider(),
    "client_id": "lakehouse-msk-producer",
}


def create_topic():
    admin = KafkaAdminClient(**CLIENT_CONFIG)
    try:
        #one replica per broker (2 brokers, one per AZ)
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=3, replication_factor=2)])
        print(f"Created topic {TOPIC}")
    except TopicAlreadyExistsError:
        print(f"Topic {TOPIC} already exists")
    finally:
        admin.close()


def handler(event, context):
    messages = event.get("messages", [])

    create_topic()

    producer = KafkaProducer(
        **CLIENT_CONFIG,
        acks="all",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    for message in messages:
        producer.send(TOPIC, message)
    producer.flush()
    producer.close()

    print(f"Published {len(messages)} messages to {TOPIC}")
    return {"topic": TOPIC, "published": len(messages)}
