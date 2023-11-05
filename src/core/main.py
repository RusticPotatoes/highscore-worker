import asyncio
import json
import logging
from asyncio import Queue

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from config import settings
from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import Insert, Select, Update

from src.database.database import get_session
from src.database.models.highscores import PlayerHiscoreData

logger = logging.getLogger(__name__)


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


# TODO: pydantic data
async def insert_highscore(session: AsyncSession, data):
    table = PlayerHiscoreData
    # check if exists first
    select_query: Select = select(table=table)
    # select_query.where(table.ts_date == )
    query: Insert = insert(table=table)
    await session.execute(query)
    pass


# TODO: pydantic data
async def update_player(session: AsyncSession, data):
    table = ""
    query: Insert = insert(table=table)
    await session.execute(query)


async def process_data(receive_queue: Queue):
    while True:
        if receive_queue.empty():
            await asyncio.sleep(1)
            continue

        message: dict = await receive_queue.get()
        highscore = message.get("hiscore")
        player = message.get("player")

        async with get_session() as session:
            session: AsyncSession
            await insert_highscore(session=session, data=highscore)
            await update_player(session=session, data=player)

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
    asyncio.run(main())
