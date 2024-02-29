from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.repositories.abc import ABCRepo
from src.app.schemas.input.skills import PlayerSkills
from src.database.models.skills import PlayerSkills as PlayerSkillsDB
from src.database.models.skills import Skills as SkillsDB


class SkillsRepo(ABCRepo):
    """Repository for managing skill data."""

    def __init__(self) -> None:
        """Initializes the SkillsRepo instance."""
        super().__init__()

    async def request(self, skill_id: int = None) -> list[SkillsDB]:
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
            data = await data.all()
        return data


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
