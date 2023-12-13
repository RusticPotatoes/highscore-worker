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
    counter: int, start_time: float, receive_queue: Queue
) -> tuple[float, int]:
    end_time = time.time()
    delta_time = end_time - start_time
    speed = counter / delta_time
    logger.info(
        f"qsize={receive_queue.qsize()}, processed {counter} in {delta_time:.2f} seconds, {speed:.2f} msg/sec"
    )
    return time.time(), 0


# Define a function to process data from a queue
async def process_data(receive_queue: Queue, error_queue: Queue):
    # Initialize counter and start time
    counter = 0
    start_time = time.time()

    # Run indefinitely
    while True:
        # Check if both queues are empty
        if receive_queue.empty():
            # If there were previous iterations with data, log processing speed
            if counter > 0:
                start_time, counter = log_speed(
                    counter=counter, start_time=start_time, receive_queue=receive_queue
                )
            # Sleep for 1 second and then continue to the next iteration
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

        # Log processing speed every 100 iterations
        if counter % 100 == 0 and counter > 0:
            start_time, counter = log_speed(
                counter=counter, start_time=start_time, receive_queue=receive_queue
            )
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
