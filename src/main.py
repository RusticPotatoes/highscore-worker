import asyncio
import json
from asyncio import Queue

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config import settings


async def kafka_consumer(topic: str, group: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=[settings.KAFKA_HOST],
        group_id=group,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    return consumer


async def kafka_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=[settings.KAFKA_HOST],
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await producer.start()
    return producer


async def receive_messages(consumer: AIOKafkaConsumer, receive_queue: Queue):
    async for message in consumer:
        value = message.value
        await receive_queue.put(value)


async def send_messages(topic: str, producer: AIOKafkaProducer, send_queue: Queue):
    while True:
        if send_queue.empty():
            await asyncio.sleep(1)
            continue
        message = await send_queue.get()
        await producer.send(topic, value=message)
        send_queue.task_done()


async def process_data(receive_queue: Queue):
    while True:
        if receive_queue.empty():
            await asyncio.sleep(1)
            continue
        message = await receive_queue.get()
        # TODO insert message into database
        receive_queue.task_done()


async def main():
    # get kafka engine
    consumer = await kafka_consumer(topic="scraper", group="highscore-worker")
    producer = await kafka_producer()

    receive_queue = Queue(maxsize=100)
    send_queue = Queue(maxsize=100)

    asyncio.create_task(receive_messages(consumer, receive_queue))
    asyncio.create_task(
        send_messages(topic="scraper", producer=producer, send_queue=send_queue)
    )

    await process_data(receive_queue)


if __name__ == "__main__":
    # TODO: async run
    main()
