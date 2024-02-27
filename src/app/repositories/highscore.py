from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.abc import ABCRepo
from app.schemas.input.highscore import PlayerHiscoreData
from app.schemas.input.player import Player
from database.models.highscores import PlayerHiscoreData as PlayerHiscoreDataDB
from database.models.player import Player as PlayerDB


class HighscoreRepo(ABCRepo):
    """Repository for managing highscore data."""

    def __init__(self) -> None:
        """Initializes the HighscoreRepo instance."""
        super().__init__()

    async def request(self, id: list[int] = None) -> list[PlayerHiscoreDataDB]:
        """Retrieves highscore data from the database.

        Args:
            id: Optional list of highscore IDs to filter by.

        Returns:
            A list of PlayerHiscoreDataDB objects representing the retrieved highscores.
        """
        table = PlayerHiscoreDataDB
        sql = select(table)
        if id:
            sql = sql.where(table.id.in_(id))

        async with self._get_session() as session:
            session: AsyncSession  # Type annotation for clarity
            data = await session.execute(sql)
            data = await data.all()
        return data

    async def create(
        self, highscore_data: list[PlayerHiscoreData], player_data: list[Player]
    ) -> None:
        """Creates new highscore entries and updates player data in the database.

        Args:
            highscore_data: A list of PlayerHiscoreData objects containing highscore information.
            player_data: A list of Player objects containing player information to update.
        """
        table_highscore = PlayerHiscoreDataDB
        table_player = PlayerDB
        highscore_data = [d.model_dump() for d in highscore_data]

        sql_insert = insert(table_highscore).values(highscore_data)
        sql_update = update(table_player)
        async with self._get_session() as session:
            session: AsyncSession  # Type annotation for clarity
            await session.execute(sql_insert)  # Insert highscore data

            for player in player_data:
                sql_update = sql_update.where(table_player.id == player.id)
                sql_update = sql_update.values(player.model_dump())
                await session.execute(sql_update)  # Update player data
