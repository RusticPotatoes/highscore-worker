import asyncio
import json
import logging
import time
import traceback
from asyncio import Queue
from datetime import datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Insert, Select, Update

import core.logging  # for log formatting
from app.schemas.highscores import playerHiscoreData as playerHiscoreDataSchema
from core.config import settings
from database.database import get_session
from database.models.highscores import PlayerHiscoreData
from database.models.player import Player

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
async def insert_highscore(session: AsyncSession, data: dict):
    player_id = data.get("Player_id")
    timestamp = data.get("timestamp")

    # Convert the timestamp to a date format
    ts_date = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").date()

    table = PlayerHiscoreData

    select_query: Select = select(table).where(
        (table.Player_id == player_id) & (table.ts_date == ts_date)
    )

    # Check if the record exists
    existing_record = await session.execute(select_query)
    existing_record = existing_record.scalars().first()

    if not existing_record:
        data = playerHiscoreDataSchema(**data)
        data = data.model_dump(mode="json")
        insert_query: Insert = insert(table).values(data).prefix_with("ignore")
        await session.execute(insert_query)


# TODO: pydantic data
async def update_player(session: AsyncSession, data: dict):
    player_id = data.get("id")

    table = Player
    query: Update = update(table=table)
    query = query.where(Player.id == player_id)
    query = query.values(data)
    await session.execute(query)


async def process_data(receive_queue: Queue):
    counter = 0
    start_time = time.time()

    while True:
        if receive_queue.empty():
            if counter > 0:
                end_time = time.time()
                delta_time = end_time - start_time
                speed = counter / delta_time
                logger.info(f"qsize={receive_queue.qsize()}, {speed:.2f} it/s")
                # reset
                start_time = time.time()
                counter = 0
            # sleep
            await asyncio.sleep(1)
            continue

        message: dict = await receive_queue.get()
        highscore: dict = message.get("hiscores")
        player: dict = message.get("player")

        if settings.ENV != "PRD":
            player_id = player.get("id")
            if player_id == 0 or player_id > 300:
                continue
        try:
            session: AsyncSession = await get_session()
            async with session.begin():
                if highscore:
                    await insert_highscore(session=session, data=highscore)
                await update_player(session=session, data=player)
                await session.commit()
        except Exception as e:
            logger.error({"error": e})
            logger.debug(f"Traceback: \n{traceback.format_exc()}")
            await receive_queue.put(message)
            receive_queue.task_done()
            continue

        if counter % 100 == 0 and counter > 0:
            end_time = time.time()
            delta_time = end_time - start_time
            speed = counter / delta_time
            logger.info(f"qsize={receive_queue.qsize()}, {speed:.2f} it/s")
            # reset
            start_time = time.time()
            counter = 0
        counter += 1


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
