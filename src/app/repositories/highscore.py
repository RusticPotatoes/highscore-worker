from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.repositories.abc import ABCRepo
from src.app.schemas.input.highscore import PlayerHiscoreData
from src.database.models.highscores import PlayerHiscoreData as PlayerHiscoreDataDB


class HighscoreRepo(ABCRepo):
    def __init__(self) -> None:
        super().__init__()

    async def request(self, id: list[int] = None):
        table = PlayerHiscoreDataDB
        sql = select(table)
        sql = sql.where(table.id.in_(id)) if id else sql

        async with self._get_session() as session:
            session: AsyncSession
            data = await session.execute(sql)
            data = await data.all()
        return data

    async def create(self, data: list[PlayerHiscoreData]):
        data = [d.model_dump() for d in data]
        table = PlayerHiscoreDataDB
        sql = insert(table)
        async with self._get_session() as session:
            session: AsyncSession
            await session.execute(sql)
        return None
