from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.repositories.abc import ABCRepo
from src.app.schemas.input.activities import PlayerActivities
from src.database.models.activities import Activities as ActivitiesDB
from src.database.models.activities import PlayerActivities as PlayerActivitiesDB


class ActivitiesRepo(ABCRepo):
    """Repository for managing activity data."""

    def __init__(self) -> None:
        """Initializes the ActivitiesRepo instance."""
        super().__init__()

    async def request(self, activity_id: int = None) -> list[ActivitiesDB]:
        """Retrieves activity data from the database.

        Args:
            activity_id: Optional activity ID to filter by.

        Returns:
            A list of ActivitiesDB objects representing the retrieved activities.
        """
        table = ActivitiesDB
        sql = select(table)

        if activity_id:
            sql = sql.where(table.activity_id == activity_id)

        async with await self._get_session() as session:
            session: AsyncSession  # Type annotation for clarity
            data = await session.execute(sql)
            data = await data.all()
        return data


class PlayerActivitiesRepo(ABCRepo):
    """Repository for managing player activity data."""

    def __init__(self) -> None:
        """Initializes the PlayerActivitiesRepo instance."""
        super().__init__()

    async def create(self, data: list[PlayerActivities]) -> None:
        """Creates new player activity entries in the database.

        Args:
            data: A list of PlayerActivities objects containing player activity information.
        """
        data = [d.model_dump() for d in data]
        table = PlayerActivitiesDB
        sql = insert(table).values(data)

        async with await self._get_session() as session:
            session: AsyncSession  # Type annotation for clarity
            await session.execute(sql)
