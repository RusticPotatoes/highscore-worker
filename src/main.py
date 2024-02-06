import asyncio
import json
import logging
import time
import traceback
from asyncio import Queue

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.schemas.highscores import playerHiscoreData as playerHiscoreDataSchema
from core.config import settings
from database.database import get_session
from database.models.highscores import PlayerHiscoreData
from database.models.player import Player
from sqlalchemy import insert, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Insert, Update

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

async def insert_data(batch: list[dict], error_queue:Queue):
    try:
        highscores:list[dict] = [msg.get("hiscores") for msg in batch]
        players:list[dict] = [msg.get("player") for msg in batch]

        highscores = [playerHiscoreDataSchema(**hs) for hs in highscores if hs]
        highscores = [hs.model_dump(mode="json") for hs in highscores ]

        session: AsyncSession = await get_session()
        
        logger.info(f"Received: {len(players)=}, {len(highscores)=}")
        
        # start a transaction
        async with session.begin():
            # insert into table values ()
            insert_sql:Insert = insert(PlayerHiscoreData)
            insert_sql = insert_sql.values(highscores)
            insert_sql = insert_sql.prefix_with("ignore")
            await session.execute(insert_sql)
            # update table
            for player in players:
                update_sql:Update = update(Player)
                update_sql = update_sql.where(Player.id == player.get("id"))
                update_sql = update_sql.values(player)
                await session.execute(update_sql)
    except OperationalError as e:
        for message in batch:
            await error_queue.put(message)

        logger.error({"error": e})
        logger.info(f"error_qsize={error_queue.qsize()}, {message=}")
    except Exception as e:
        for message in batch:
            await error_queue.put(message)

        logger.error({"error": e})
        logger.debug(f"Traceback: \n{traceback.format_exc()}")
        logger.info(f"error_qsize={error_queue.qsize()}, {message=}")

async def process_data(receive_queue: Queue, error_queue: Queue):
    # Initialize counter and start time
    counter = 0
    start_time = time.time()

    batch = []
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
        
        #TODO fix test data
        if settings.ENV != "PRD":
            player = message.get("player")
            player_id = player.get("id")
            MIN_PLAYER_ID = 0
            MAX_PLAYER_ID = 300
            if not (MIN_PLAYER_ID < player_id <= MAX_PLAYER_ID):
                continue
        
        # batch message
        batch.append(message)

        now = time.time()

        if len(batch) > 100 or now-start_time > 5:
            await insert_data(batch=batch, error_queue=error_queue)
        
        receive_queue.task_done()
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
