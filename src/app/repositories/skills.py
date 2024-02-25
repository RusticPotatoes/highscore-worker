from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.repositories.abc import ABCRepo
from src.app.schemas.input.skills import PlayerSkills
from src.database.models.skills import PlayerSkills as PlayerSkillsDB
from src.database.models.skills import Skills as SkillsDB


class SkillsRepo(ABCRepo):
    def __init__(self) -> None:
        super().__init__()

    async def request(self, skill_id: int = None):
        table = SkillsDB
        sql = select(table)
        if skill_id:
            sql = sql.where(table.skill_id == skill_id)
        async with self._get_session() as session:
            session: AsyncSession
            data = await session.execute(sql)
            data = await data.all()
        return data


class PlayerSkillsRepo(ABCRepo):
    def __init__(self) -> None:
        super().__init__()

    async def create(self, data: list[PlayerSkills]):
        data = [d.model_dump() for d in data]
        table = PlayerSkillsDB
        sql = insert(table).values(data)
        async with self._get_session() as session:
            session: AsyncSession
            await session.execute(sql)
        return None
