import asyncio
import json
import logging
import time
import traceback
from asyncio import Queue
from datetime import datetime, timedelta


from core.config import settings
from database.database import get_session
from database.models.highscores import PlayerHiscoreData
from database.models.player import Player
from database.models.skills import PlayerSkills as PlayerSkillsDB, Skills as SkillsDB
from database.models.activities import PlayerActivities as PlayerActivitiesDB, Activities as ActivitiesDB
from pydantic import BaseModel
from sqlalchemy import insert, update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Insert, Update

import my_kafka as my_kafka
# schemas import
from app.repositories.highscore import HighscoreRepo
from app.schemas.input.highscore import PlayerHiscoreData
from app.schemas.input.player import Player
from app.schemas.input.scraper_data import ScraperData
from app.schemas.input.activities import Activities, PlayerActivities
from app.schemas.input.skills import Skills, PlayerSkills

logger = logging.getLogger(__name__)


class Message(BaseModel):
    hiscores: PlayerHiscoreData | None
    player: Player | None


class NewDataSchema(BaseModel):
    scraper_data: ScraperData
    player_skills: list[PlayerSkills]
    player_activities: list[PlayerActivities]
    player: Player


# Global variables to cache the skill and activity names
SKILL_NAMES: list[Skills] = []
ACTIVITY_NAMES: list[Activities] = []
# Global variables for the locks
SKILL_NAMES_LOCK = asyncio.Lock()
ACTIVITY_NAMES_LOCK = asyncio.Lock()
# Global variable to track when the cache was last updated
LAST_SKILL_NAMES_UPDATE = datetime.min
LAST_ACTIVITY_NAMES_UPDATE = datetime.min


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

async def insert_data_v1(batch: list[Message], error_queue: Queue):
    try:
        highscores:list[dict] = [msg.get("hiscores") for msg in batch]
        players:list[dict] = [msg.get("player") for msg in batch]

        highscores = [PlayerHiscoreData(**hs) for hs in highscores if hs]
        highscores = [hs.model_dump(mode="json") for hs in highscores ]

        session: AsyncSession = await get_session()
        
        logger.info(f"Received: {len(players)=}, {len(highscores)=}")

        # start a transaction
        async with session.begin():
            # insert into table values ()
            insert_sql:Insert = insert(PlayerHiscoreData) # fixing v1, currently debugging here
            insert_sql = insert_sql.values(highscores)
            insert_sql = insert_sql.prefix_with("ignore")
            await session.execute(insert_sql)
            # update table
            for player in players:
                update_sql:Update = update(Player)
                update_sql = update_sql.where(Player.id == player.get("id"))
                update_sql = update_sql.values(player)
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


async def check_and_update_skill_cache(batch: list[Message], session: AsyncSession):
    global SKILL_NAMES, LAST_SKILL_NAMES_UPDATE, SKILL_NAMES_LOCK, ACTIVITY_NAMES

    # Query the cache to get the skill IDs
    skill_ids = {skill.name: skill.id for skill in SKILL_NAMES} if SKILL_NAMES else {}

    missing_skills = [
        skill
        for message in batch
        for skill in message.hiscores.model_fields.keys()
        if skill
        not in ["timestamp", "Player_id"] + [skill.skill_name for skill in SKILL_NAMES]
        and skill not in skill_ids
    ]
    if missing_skills:
        # Check if the cache was updated less than 10 minutes ago
        if datetime.now() - LAST_SKILL_NAMES_UPDATE < timedelta(minutes=10):
            logger.warning(
                "Skill names cache update was called less than 10 minutes ago. Skipping batch."
            )
            return None  # Or however you want to handle this case

        # Update the skill names cache
        async with SKILL_NAMES_LOCK:
            await update_skill_names(session)
        LAST_SKILL_NAMES_UPDATE = datetime.now()

        # Query the cache again to get the updated skill IDs
        skill_ids = (
            {skill.name: skill.id for skill in SKILL_NAMES} if SKILL_NAMES else {}
        )

    return skill_ids


async def check_and_update_activity_cache(batch: list[Message], session: AsyncSession):
    global ACTIVITY_NAMES, LAST_ACTIVITY_NAMES_UPDATE, ACTIVITY_NAMES_LOCK, SKILL_NAMES

    # Query the cache to get the activity IDs
    activity_ids = (
        {activity.name: activity.id for activity in ACTIVITY_NAMES}
        if ACTIVITY_NAMES
        else {}
    )

    # Check if any activity name in any message is not found in the cache
    missing_activities = [
        activity
        for message in batch
        for activity in message.hiscores.model_fields.keys()
        if activity
        not in ["timestamp", "Player_id"] + [skill.skill_name for skill in SKILL_NAMES]
        and activity not in activity_ids
    ]
    if missing_activities:
        # Check if the cache was updated less than 10 minutes ago
        if datetime.now() - LAST_ACTIVITY_NAMES_UPDATE < timedelta(minutes=10):
            logger.warning(
                "Activity names cache update was called less than 10 minutes ago. Skipping batch."
            )
            return None  # Or however you want to handle this case

        # Update the activity names cache
        async with ACTIVITY_NAMES_LOCK:
            await update_activity_names(session)
        LAST_ACTIVITY_NAMES_UPDATE = datetime.now()

        # Query the cache again to get the updated activity IDs
        activity_ids = (
            {activity.name: activity.id for activity in ACTIVITY_NAMES}
            if ACTIVITY_NAMES
            else {}
        )

    return activity_ids


async def insert_data_v2(batch: list[Message], error_queue: Queue):
    """
    1. check for duplicates in scraper_data[player_id, record_date], remove all duplicates
    2. start transaction
    3. for each player insert into scraper_data
    4. for each player get the scraper_id from scraper_data
    5. insert into player_skills (scraper_id, skill_id) values (), ()
    6. insert into player_activities (scraper_id, activity_id) values (), ()

    step 5 & 6 must be batched for all players at once
    """
    try:
        session: AsyncSession = await get_session()

        # Step 1: Check for duplicates in scraper_data[player_id, record_date], remove all duplicates
        for message in batch:
            existing_data = await session.query(ScraperData).filter(
                ScraperData.player_id == message.player_id,
                ScraperData.record_date == message.record_date
            ).first()
            if existing_data:
                session.delete(existing_data)
                await session.commit()

        # Step 2: Start transaction
        async with session.begin():
            for message in batch:
                # Step 3: For each player insert into scraper_data
                scraper_data = ScraperData(
                    player_id=message.player_id,
                    record_date=message.record_date
                )
                session.add(scraper_data)
                await session.flush()

                # Step 4: For each player get the scraper_id from scraper_data
                scraper_id = scraper_data.scraper_id

                # Step 5 & 6: Insert into player_skills and player_activities
                # Assuming you have the skills and activities data in the message
                player_skills = [PlayerSkill(scraper_id=scraper_id, skill_id=skill_id) for skill_id in message.skills]
                player_activities = [PlayerActivity(scraper_id=scraper_id, activity_id=activity_id) for activity_id in message.activities]

                session.bulk_save_objects(player_skills)
                session.bulk_save_objects(player_activities)

            # Commit the transaction
            await session.commit()
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

async def transform_data(
    old_data_list: list[Message], session: AsyncSession
) -> NewDataSchema:
    global SKILL_NAMES, ACTIVITY_NAMES, LAST_CACHE_UPDATE

    new_data_list = []

    for old_data in old_data_list:
        # Query the cache to get the skill and activity IDs
        skill_ids = (
            {skill.name: skill.id for skill in SKILL_NAMES} if SKILL_NAMES else {}
        )
        activity_ids = (
            {activity.name: activity.id for activity in ACTIVITY_NAMES}
            if ACTIVITY_NAMES
            else {}
        )

        # Transform the old data format into the new format
        new_data = NewDataSchema(
            **{
                "scraper_data": {
                    "scraper_id": old_data.player.id if old_data.player else None,
                    "created_at": (
                        old_data.hiscores.timestamp.isoformat()
                        if old_data.hiscores
                        else None
                    ),
                    "player_id": (
                        old_data.hiscores.Player_id if old_data.hiscores else None
                    ),
                    "record_date": (
                        datetime.utcnow().isoformat() if old_data.hiscores else None
                    ),
                },
                "player_skills": (
                    [
                        {
                            "skill_id": (
                                skill_ids[skill.name]
                                if skill.name in skill_ids
                                else None
                            ),
                            "skill_value": (
                                getattr(old_data.hiscores, skill.name, None)
                                if old_data.hiscores
                                else None
                            ),
                        }
                        for skill in SKILL_NAMES
                    ]
                    if SKILL_NAMES
                    else []
                ),
                "player_activities": (
                    [
                        {
                            "activity_id": (
                                activity_ids[activity.name]
                                if activity.name in activity_ids
                                else None
                            ),
                            "activity_value": (
                                getattr(old_data.hiscores, activity.name, None)
                                if old_data.hiscores
                                else None
                            ),
                        }
                        for activity in ACTIVITY_NAMES
                    ]
                    if ACTIVITY_NAMES
                    else []
                ),
                "player": {
                    "id": old_data.hiscores.Player_id if old_data.hiscores else None,
                    "name": old_data.player.name if old_data.player else None,
                    "normalized_name": (
                        old_data.player.normalized_name if old_data.player else None
                    ),
                },
            }
        )

        logger.debug(f"Transformed data: {new_data}")
        new_data_list.append(new_data)

    return new_data_list
## todo: verify this is rigth
async def update_skill_names(session: AsyncSession):
    global SKILL_NAMES, SKILL_NAMES_LOCK

    async with SKILL_NAMES_LOCK:
        if SKILL_NAMES is None:
            skill_records = await session.execute(select(SkillsDB))
            SKILL_NAMES = [
                SkillsDB(**record) for record in skill_records.scalars().all()
            ]


async def update_activity_names(session: AsyncSession):
    global ACTIVITY_NAMES, ACTIVITY_NAMES_LOCK

    async with ACTIVITY_NAMES_LOCK:
        if ACTIVITY_NAMES is None:
            activity_records = await session.execute(select(ActivitiesDB))
            ACTIVITY_NAMES = [
                ActivitiesDB(**record)
                for record in activity_records.scalars().all()
            ]

async def process_data_v1(receive_queue: Queue, error_queue: Queue):
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
            interval=15
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

        # insert data in batches of N or interval of N
        if len(batch) > 100 or now-start_time > 15:
            async with semaphore:
                await insert_data_v1(batch=batch, error_queue=error_queue)
            batch = []
        
        receive_queue.task_done()
        counter += 1

async def process_data_v2(receive_queue: Queue, error_queue: Queue):
    # Initialize counter and start time
    counter = 0
    start_time = time.time()

    # limit the number of async insert_data calls
    semaphore = asyncio.Semaphore(5)

    batch: list[Message] = []
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
        message: Message = Message(**message)

        # TODO fix test data
        if settings.ENV != "PRD":
            continue_flag = False
            MIN_PLAYER_ID = 0
            MAX_PLAYER_ID = 300
            # original

            player = message.player
            player_id = player.id  # Access the 'id' attribute directly
            
            if not (MIN_PLAYER_ID < player_id <= MAX_PLAYER_ID):
                continue

        # batch message
        batch.append(message)

        now = time.time()

        # insert data in batches of N or interval of N
        if len(batch) > 100 or now - start_time > 15:
            async with semaphore:
                await insert_data_v2(batch=batch, error_queue=error_queue)
            batch = []

        receive_queue.task_done()
        counter += 1


async def main():
    # get kafka engine
    consumer = await my_kafka.kafka_consumer(topic="scraper", group="highscore-worker")
    producer = await my_kafka.kafka_producer()

    receive_queue = Queue(maxsize=100)
    send_queue = Queue(maxsize=100)

    asyncio.create_task(
        my_kafka.receive_messages(
            consumer=consumer, receive_queue=receive_queue, error_queue=send_queue
        )
    )
    asyncio.create_task(
        my_kafka.send_messages(topic="scraper", producer=producer, send_queue=send_queue)
    )
    asyncio.create_task(
        process_data_v1(receive_queue=receive_queue, error_queue=send_queue)
    )

    # asyncio.create_task(
    #     process_data_v2(receive_queue=receive_queue, error_queue=send_queue)
    # )

    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
