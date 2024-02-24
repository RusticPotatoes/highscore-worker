import asyncio
import json
import logging
import time
import traceback
from asyncio import Queue

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select
from app.schemas.highscores import (
    playerHiscoreData as playerHiscoreDataSchema,
    PlayerActivityCreate,
    ScraperDataCreate,
    PlayerSkillCreate,
    PlayerCreate,
)
from core.config import settings
from database.database import get_session
from database.models.highscores import (
    # PlayerHiscoreData,
    PlayerActivities,
    PlayerSkills,
    ScraperData,
)
from database.models.player import Player
from sqlalchemy import insert, update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Insert, Update

logger = logging.getLogger(__name__)

# Global variables to cache the skill and activity names
SKILL_NAMES = None
ACTIVITY_NAMES = None
# Global lock for updating the cache
CACHE_UPDATE_LOCK = asyncio.Lock()


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


async def insert_data(batch: list[dict], error_queue: Queue):
    session: AsyncSession = await get_session()

    try:
        # Transform the old data format into the new format
        batch = [await transform_data(msg, session) for msg in batch]

        scraper_data_list: list[dict] = [msg.get("scraper_data") for msg in batch]
        player_skills_list: list[dict] = [msg.get("player_skills") for msg in batch]
        player_activities_list: list[dict] = [
            msg.get("player_activities") for msg in batch
        ]
        players: list[dict] = [msg.get("player") for msg in batch]

        scraper_data_list = [ScraperDataCreate(**sd) for sd in scraper_data_list if sd]
        player_skills_list = [
            PlayerSkillCreate(**ps) for ps in player_skills_list if ps
        ]
        player_activities_list = [
            PlayerActivityCreate(**pa) for pa in player_activities_list if pa
        ]
        players = [PlayerCreate(**p) for p in players if p]

        logger.info(
            f"Received: players={len(players)}, scraper_data={len(scraper_data_list)}, skills={len(player_skills_list)}, activities={len(player_activities_list)}"
        )

        # start a transaction
        async with session.begin():
            # insert into scraper_data table
            for scraper_data in scraper_data_list:
                insert_scraper_data = (
                    insert(ScraperData)
                    .values(scraper_data.dict())
                    .prefix_with("ignore")
                )
                await session.execute(insert_scraper_data)

            # insert into player_skills table
            for player_skill in player_skills_list:
                insert_player_skill = (
                    insert(PlayerSkills)
                    .values(player_skill.dict())
                    .prefix_with("ignore")
                )
                await session.execute(insert_player_skill)

            # insert into player_activities table
            for player_activity in player_activities_list:
                insert_player_activity = (
                    insert(PlayerActivities)
                    .values(player_activity.dict())
                    .prefix_with("ignore")
                )
                await session.execute(insert_player_activity)

            # update Player table
            for player in players:
                update_sql: Update = update(Player)
                update_sql = update_sql.where(Player.id == player.id)
                update_sql = update_sql.values(player.dict())
                await session.execute(update_sql)
    except (OperationalError, IntegrityError) as e:
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


async def transform_data(old_data: dict, session: AsyncSession) -> dict:
    global SKILL_NAMES, ACTIVITY_NAMES

    # Fetch the skill and activity names from the database if they're not already cached
    async with CACHE_UPDATE_LOCK:
        if SKILL_NAMES is None:
            skill_names = await session.execute(
                select(PlayerSkills.skill_value)
            )  # Update this line
            SKILL_NAMES = [result[0] for result in skill_names.scalars().all()]
        if ACTIVITY_NAMES is None:
            activity_names = await session.execute(
                select(PlayerActivities.activity_value)
            )  # And this line
            ACTIVITY_NAMES = [result[0] for result in activity_names.scalars().all()]

    # Transform the old data format into the new format
    new_data = {
        "scraper_data": {
            "scraper_id": old_data.get("id"),
            "created_at": old_data.get("timestamp"),
            "player_id": old_data.get("Player_id"),
            "record_date": old_data.get("ts_date"),
        },
        "player_skills": [
            {"skill_id": i, "skill_value": old_data.get(skill)}
            for i, skill in enumerate(SKILL_NAMES)
        ],
        "player_activities": [
            {"activity_id": i, "activity_value": old_data.get(activity)}
            for i, activity in enumerate(ACTIVITY_NAMES)
        ],
        "player": {
            "id": old_data.get("Player_id"),
        },
    }
    return new_data


async def update_cache(session: AsyncSession):
    global SKILL_NAMES, ACTIVITY_NAMES

    # Fetch the skill and activity names from the database
    skill_names = await session.execute(select(PlayerSkills.skill_name))
    SKILL_NAMES = [result[0] for result in skill_names.scalars().all()]
    activity_names = await session.execute(select(PlayerActivities.activity_name))
    ACTIVITY_NAMES = [result[0] for result in activity_names.scalars().all()]


async def process_data(receive_queue: Queue, error_queue: Queue):
    # Initialize counter and start time
    counter = 0
    start_time = time.time()

    # limit the number of async insert_data calls
    semaphore = asyncio.Semaphore(5)

    batch = []
    # Run indefinitely
    while True:
        start_time, counter = log_speed(
            counter=counter,
            start_time=start_time,
            _queue=receive_queue,
            topic="scraper",
            interval=15,
        )

        # Check if queue is empty
        if receive_queue.empty():
            await asyncio.sleep(1)
            continue

        # Get a message from the chosen queue
        message: dict = await receive_queue.get()

        # TODO fix test data
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

        # insert data in batches of N or interval of N
        if len(batch) > 100 or now - start_time > 15:
            async with semaphore:
                await insert_data(batch=batch, error_queue=error_queue)
            batch = []

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
