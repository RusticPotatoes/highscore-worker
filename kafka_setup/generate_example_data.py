import asyncio
import json
from asyncio import Queue

from AioKafkaEngine.AioKafkaEngine import AioKafkaEngine


async def generate_example_data(queue: Queue):
    count = 0
    while True:
        item: dict = await queue.get()

        if item is None or count >= 1000:
            break

        player = item.get("player")
        highscore = item.get("hiscores")

        if player:
            item["player"]["id"] = count

        if highscore:
            item["hiscores"]["Player_id"] = count

        print("," + json.dumps(item))

        queue.task_done()
        count += 1


async def main():
    kafka_engine = AioKafkaEngine(bootstrap_servers=["localhost:9094"], topic="scraper")
    await kafka_engine.start_consumer(group_id="test")
    await kafka_engine.consume_messages()

    await generate_example_data(kafka_engine.receive_queue)

    await kafka_engine.stop_consumer()


if __name__ == "__main__":
    asyncio.run(main())
