import asyncio
import json
import logging
import time
import traceback
from asyncio import Queue
from datetime import datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import insert, select, update
from sqlalchemy.exc import OperationalError
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
        acks="all",
    )
    await producer.start()
    return producer


async def receive_messages(
    consumer: AIOKafkaConsumer, receive_queue: Queue, error_queue: Queue
):
    async for message in consumer:
        if error_queue.qsize() > 100:
            await asyncio.sleep(1)
            continue
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


def log_speed(
    counter: int, start_time: float, _queue: Queue, topic: str, interval: int = 15
) -> tuple[float, int]:
    # Calculate the time elapsed since the function started
    delta_time = time.time() - start_time

    # Check if the specified interval has not elapsed yet
    if delta_time < interval:
        # Return the original start time and the current counter value
        return start_time, counter

    # Calculate the processing speed (messages per second)
    speed = counter / delta_time

    # Log the processing speed and relevant information
    log_message = (
        f"{topic=}, qsize={_queue.qsize()}, "
        f"processed {counter} in {delta_time:.2f} seconds, {speed:.2f} msg/sec"
    )
    logger.info(log_message)

    # Return the current time and reset the counter to zero
    return time.time(), 0


# Define a function to process data from a queue
async def process_data(receive_queue: Queue, error_queue: Queue):
    # Initialize counter and start time
    counter = 0
    start_time = time.time()

    # Run indefinitely
    while True:
        start_time, counter = log_speed(
            counter=counter,
            start_time=start_time,
            _queue=receive_queue,
            topic="scraper",
        )

        # Check if queue is empty
        if receive_queue.empty():
            await asyncio.sleep(1)
            continue

        # Get a message from the chosen queue
        message: dict = await receive_queue.get()

        # Extract 'hiscores' and 'player' dictionaries from the message
        highscore: dict = message.get("hiscores")
        player: dict = message.get("player")

        # Check the environment and filter out certain player IDs if not in production
        if settings.ENV != "PRD":
            player_id = player.get("id")
            MIN_PLAYER_ID = 0
            MAX_PLAYER_ID = 300
            if not (MIN_PLAYER_ID < player_id <= MAX_PLAYER_ID):
                continue

        try:
            # Acquire an asynchronous database session
            session: AsyncSession = await get_session()
            async with session.begin():
                # If 'highscore' dictionary is present, insert it into the database
                if highscore:
                    await insert_highscore(session=session, data=highscore)
                # Update the player information in the database
                await update_player(session=session, data=player)
                # Commit the changes to the database
                await session.commit()
            # Mark the message as processed in the queue
            receive_queue.task_done()
        except OperationalError as e:
            await error_queue.put(message)
            # Handle exceptions, log the error, and put the message in the error queue
            logger.error({"error": e})
            logger.info(f"error_qsize={error_queue.qsize()}, {message=}")
            # Mark the message as processed in the queue and continue to the next iteration
            receive_queue.task_done()
            continue
        except Exception as e:
            await error_queue.put(message)
            # Handle exceptions, log the error, and put the message in the error queue
            logger.error({"error": e})
            logger.debug(f"Traceback: \n{traceback.format_exc()}")
            logger.info(f"error_qsize={error_queue.qsize()}, {message=}")
            # Mark the message as processed in the queue and continue to the next iteration
            receive_queue.task_done()
            continue

        # Increment the counter
        counter += 1


async def main():
    # get kafka engine
    consumer = await kafka_consumer(topic="scraper", group="highscore-worker")
    producer = await kafka_producer()

    receive_queue = Queue(maxsize=100)
    send_queue = Queue(maxsize=100)

    asyncio.create_task(
        receive_messages(
            consumer=consumer, receive_queue=receive_queue, error_queue=send_queue
        )
    )
    asyncio.create_task(
        send_messages(topic="scraper", producer=producer, send_queue=send_queue)
    )
    asyncio.create_task(
        process_data(receive_queue=receive_queue, error_queue=send_queue)
    )

    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
