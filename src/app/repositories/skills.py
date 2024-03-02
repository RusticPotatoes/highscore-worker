import logging

from app.repositories.abc import ABCRepo
from app.schemas.input.skills import PlayerSkills, Skills
from database.models.skills import PlayerSkills as PlayerSkillsDB
from database.models.skills import Skills as SkillsDB
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class SkillsRepo(ABCRepo):
    """Repository for managing skill data."""

    def __init__(self) -> None:
        """Initializes the SkillsRepo instance."""
        super().__init__()

    async def create(self, data):
        return await super().create(data)

    async def request(self, skill_id: int = None) -> list[Skills]:
        """Retrieves skill data from the database.

        Args:
            skill_id: Optional skill ID to filter by.

        Returns:
            A list of SkillsDB objects representing the retrieved skills.
        """
        table = SkillsDB
        sql = select(table)

        if skill_id:
            sql = sql.where(table.skill_id == skill_id)

        async with await self._get_session() as session:
            session: AsyncSession  # Type annotation for clarity
            data = await session.execute(sql)
            data = data.scalars()
        return [Skills(**d.__dict__) for d in data]

    async def update(self, id, data):
        return await super().update(id, data)

    async def delete(self, id):
        return await super().delete(id)


class PlayerSkillsRepo(ABCRepo):
    """Repository for managing player skill data."""

    def __init__(self) -> None:
        """Initializes the PlayerSkillsRepo instance."""
        super().__init__()

    async def create(self, data: list[PlayerSkills]) -> None:
        """Creates new player skill entries in the database.

        Args:
            data: A list of PlayerSkills objects containing player skill information.
        """
        data = [d.model_dump() for d in data]
        table = PlayerSkillsDB
        sql = insert(table).values(data)

        async with await self._get_session() as session:
            session: AsyncSession  # Type annotation for clarity
            await session.execute(sql)

    async def request(self, id):
        return await super().request(id)

    async def update(self, id, data):
        return await super().update(id, data)

    async def delete(self, id):
        return await super().delete(id)
