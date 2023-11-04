# setup_kafka.py
import json
import os
import time
import zipfile
from queue import Queue

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


def create_topics():
    # Get the Kafka broker address from the environment variable
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")

    # Create Kafka topics
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

    topics = admin_client.list_topics()
    print("existing topics", topics)

    if not topics == []:
        admin_client.delete_topics(topics)

    res = admin_client.create_topics(
        [
            NewTopic(
                name="player",
                num_partitions=3,
                replication_factor=1,
            ),
            NewTopic(
                name="scraper",
                num_partitions=4,
                replication_factor=1,
            ),
            NewTopic(
                name="reports",
                num_partitions=4,
                replication_factor=1,
            ),
        ]
    )

    print("created_topic", res)

    topics = admin_client.list_topics()
    print("all topics", topics)
    return


def extract_zip(extract_to: str):
    current_dir = "./kafka_data"  # Get the current working directory

    # Find zip file in the current directory
    zip_files = [file for file in os.listdir(current_dir) if file.endswith(".zip")]

    if not zip_files:
        print("No zip file found in the current directory")
        return

    # Select the first zip file found
    for zip_file in zip_files:
        print(f"extracting: {zip_file}")
        zip_file_path = os.path.join(current_dir, zip_file)
        # Create the extraction directory if it doesn't exist
        if not os.path.exists(extract_to):
            os.makedirs(extract_to)

        # Extract zipfile
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
    return


def get_messages_from_json(path: str, send_queue: Queue):
    paths = []
    for file_name in os.listdir(path):
        print(f"{file_name=}")
        if file_name.endswith(".json"):
            file_path = os.path.join(path, file_name)
            paths.append(file_path)

    for _path in paths:
        print(f"{_path=}")
        with open(_path) as file:
            data = json.load(file)
            print(f"{_path}:{len(data)}")
            _ = [send_queue.put(item=d) for d in data]
    return


def kafka_producer():
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda x: json.dumps(x).encode(),
    )
    return producer


def send_messages(producer: KafkaProducer, send_queue: Queue, topic: str = "scraper"):
    while True:
        if send_queue.empty():
            break

        if send_queue.qsize() % 100 == 0:
            print(f"{send_queue.qsize()=}")
        message = send_queue.get()
        producer.send(topic=topic, value=message)
        send_queue.task_done()


def insert_data():
    send_queue = Queue()
    extract_to = "kafka_data"
    producer = kafka_producer()

    print("extract_zip")
    extract_zip(extract_to)
    print("get_messages_from_json")
    get_messages_from_json(extract_to, send_queue=send_queue)
    print("send_messages")
    send_messages(producer=producer, send_queue=send_queue)


def main():
    print("create_topics")
    create_topics()
    print("insert_data")
    insert_data()
    print("done")


main()
