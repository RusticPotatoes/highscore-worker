import logging

from app.repositories.abc import ABCRepo
from app.schemas.input.activities import PlayerActivities
from app.schemas.input.player import Player
from app.schemas.input.skills import PlayerSkills
from app.schemas.scraper_data import ScraperCreate, ScraperData
from database.models.activities import PlayerActivities as PlayerActivitiesDB
from database.models.player import Player as PlayerDB
from database.models.scraper_data import ScraperData as ScraperDataDB
from database.models.skills import PlayerSkills as PlayerSkillsDB
from sqlalchemy import and_, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ScraperDataRepo(ABCRepo):
    """Repository for managing skill data."""

    def __init__(self) -> None:
        """Initializes the SkillsRepo instance."""
        super().__init__()

    async def request(self, id):
        return await super().request(id)

    async def create(
        self,
        highscore_data: list[
            tuple[list[PlayerSkills], list[PlayerActivities], ScraperCreate]
        ],
        player_data: list[Player],
    ) -> None:
        table = ScraperDataDB

        async with await self._get_session() as session:
            session: AsyncSession
            skills = []
            activities = []
            for data in highscore_data:
                # insert scraperdata
                await session.execute(
                    insert(table).values(data[2].model_dump()).prefix_with("ignore")
                )
                scraper_record = await session.execute(
                    select(table).where(
                        and_(
                            table.player_id == data[2].player_id,
                            table.record_date == data[2].created_at.date(),
                        )
                    )
                )
                scraper_record = scraper_record.scalars()
                scraper_record = [ScraperData(**s.__dict__) for s in scraper_record]
                assert len(scraper_record) == 1
                scraper_record = scraper_record[0]

                for d in data[0]:
                    d.scraper_id = scraper_record.scraper_id
                    if d.skill_value > 0:
                        skills.append(d.model_dump())
                for d in data[1]:
                    d.scraper_id = scraper_record.scraper_id
                    if d.activity_value > 0:
                        activities.append(d.model_dump())

            await session.execute(
                insert(PlayerActivitiesDB).values(activities).prefix_with("ignore")
            )
            await session.execute(
                insert(PlayerSkillsDB).values(skills).prefix_with("ignore")
            )

            for player in player_data:
                await session.execute(
                    update(PlayerDB)
                    .values(player.model_dump())
                    .where(PlayerDB.id == player.id)
                )
            await session.commit()
        return None

    async def update(self, id, data):
        return await super().update(id, data)

    async def delete(self, id):
        return await super().delete(id)
