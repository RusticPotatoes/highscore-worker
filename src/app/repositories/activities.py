from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.repositories.abc import ABCRepo
from src.app.schemas.input.activities import playerActivities
from src.database.models.activities import Activities as ActivitiesDB
from src.database.models.activities import PlayerActivities as playerActivitiesDB


class ActivitiesRepo(ABCRepo):
    def __init__(self) -> None:
        super().__init__()

    async def request(self, activity_id: int = None):
        table = ActivitiesDB
        sql = select(table)

        if activity_id:
            sql = sql.where(table.activity_id == activity_id)

        async with self._get_session() as session:
            session: AsyncSession
            data = await session.execute(sql)
            data = await data.all()
        return data


class PlayerActivitiesRepo(ABCRepo):
    def __init__(self) -> None:
        super().__init__()

    async def create(self, data: list[playerActivities]):
        data = [d.model_dump() for d in data]
        table = playerActivitiesDB
        sql = insert(table).values(data)
        async with self._get_session() as session:
            session: AsyncSession
            await session.execute(sql)
        return None
