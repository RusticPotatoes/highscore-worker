import asyncio
import logging
import time
import traceback
from asyncio import Queue

import my_kafka as my_kafka
from app.repositories.activities import ActivitiesRepo

# schemas import
from app.repositories.highscore import HighscoreRepo
from app.repositories.scraper_data import ScraperDataRepo
from app.repositories.skills import SkillsRepo
from app.schemas.input.activities import Activities, PlayerActivities
from app.schemas.input.message import Message
from app.schemas.input.skills import PlayerSkills, Skills
from app.schemas.scraper_data import ScraperCreate
from core.config import settings
from sqlalchemy.exc import IntegrityError, OperationalError

logger = logging.getLogger(__name__)


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
        highscores = [msg.hiscores for msg in batch if msg.hiscores]
        players = [msg.player for msg in batch if msg.player]

        logger.info(f"Received: {len(players)=}, {len(highscores)=}")

        repo = HighscoreRepo()
        await repo.create(highscore_data=highscores, player_data=players)
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


async def insert_data_v2(batch: list[Message], error_queue: Queue):
    try:
        highscores = [msg.hiscores for msg in batch if msg.hiscores]
        players = [msg.player for msg in batch if msg.player]

        logger.info(f"Received: {len(players)=}, {len(highscores)=}")

        scraper_repo = ScraperDataRepo()

        skills_repo = SkillsRepo()
        activities_repo = ActivitiesRepo()

        skills = {s.skill_name: s for s in await skills_repo.request()}

        activities = {a.activity_name: a for a in await activities_repo.request()}

        highscore_data = []
        scraper_data = []
        for highscore in highscores:
            player_skills: list[PlayerSkills] = []
            player_activities: list[PlayerActivities] = []
            scraper_data = ScraperCreate(
                player_id=highscore.Player_id, created_at=highscore.timestamp
            )
            _highscore = highscore.model_dump()
            assert isinstance(_highscore, dict)
            logger.info(_highscore)
            for k, v in _highscore.items():
                if k in skills.keys():
                    skill = skills.get(k)
                    assert isinstance(skill, Skills)
                    player_skills.append(
                        PlayerSkills(
                            scraper_id=None, skill_id=skill.skill_id, skill_value=v
                        )
                    )
                if k in activities.keys():
                    activity = activities.get(k)
                    assert isinstance(activity, Activities)
                    player_activities.append(
                        PlayerActivities(
                            scraper_id=None,
                            activity_id=activity.activity_id,
                            activity_value=v,
                        )
                    )
            highscore_data.append((player_skills, player_activities, scraper_data))
            logger.info(f"{highscore_data[0]}, {players[0]}")
        await scraper_repo.create(highscore_data=highscore_data, player_data=players)
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
        message = await receive_queue.get()
        message = Message(**message)

        # TODO fix test data
        if settings.ENV != "PRD":
            player_id = message.player.id
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
                # await insert_data_v1(batch=batch, error_queue=error_queue)
                await insert_data_v2(batch=batch, error_queue=error_queue)
                break
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
        my_kafka.send_messages(
            topic="scraper", producer=producer, send_queue=send_queue
        )
    )
    asyncio.create_task(
        process_data(receive_queue=receive_queue, error_queue=send_queue)
    )

    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
