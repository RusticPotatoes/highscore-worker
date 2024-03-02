from app.repositories.abc import ABCRepo
from app.schemas.input.activities import Activities, PlayerActivities
from database.models.activities import Activities as ActivitiesDB
from database.models.activities import PlayerActivities as PlayerActivitiesDB
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession


class ActivitiesRepo(ABCRepo):
    """Repository for managing activity data."""

    def __init__(self) -> None:
        """Initializes the ActivitiesRepo instance."""
        super().__init__()

    async def create(self, data):
        return await super().create(data)

    async def request(self, activity_id: int = None) -> list[Activities]:
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
            data = data.scalars()
        return [Activities(**d.__dict__) for d in data]

    async def update(self, id, data):
        return await super().update(id, data)

    async def delete(self, id):
        return await super().delete(id)


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

    async def request(self, id):
        return await super().request(id)

    async def update(self, id, data):
        return await super().update(id, data)

    async def delete(self, id):
        return await super().delete(id)
