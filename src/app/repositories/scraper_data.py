from datetime import datetime

from app.schemas.scraper_data import ScraperCreate, ScraperData
from sqlalchemy import and_, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.repositories.abc import ABCRepo
from src.app.repositories.activities import PlayerActivities
from src.app.repositories.skills import PlayerSkills
from src.app.schemas.input.highscore import PlayerHiscoreData
from src.database.models.scraper_data import ScraperData as ScraperDataDB


class ScraperDataRepo(ABCRepo):
    """Repository for managing skill data."""

    def __init__(self) -> None:
        """Initializes the SkillsRepo instance."""
        super().__init__()

    async def request(self, scraper_id: int = None) -> list[ScraperData]:
        """ """
        table = ScraperDataDB
        sql = select(table)
        sql = sql.where(ScraperDataDB.scraper_id == scraper_id)

        async with await self._get_session() as session:
            session: AsyncSession
            data = await session.execute(sql)
            data = await data.all()
        return [ScraperData(**d) for d in data]

    async def create(
        self, highscore_data: list[PlayerHiscoreData], player_data: list
    ) -> None:
        repo_skills = PlayerSkills()
        repo_activities = PlayerActivities()

        date_fmt = "%Y-%m-%d %HH:%MM:%SS"
        data = [
            {
                "record": ScraperCreate(
                    player_id=d.Player_id,
                    created_at=datetime.strptime(d.timestamp, date_fmt),
                ),
                "highscore": d.model_dump(),
            }
            for d in highscore_data
        ]

        table = ScraperDataDB

        sql_insert = insert(table)
        sql_insert = sql_insert.values([d["record"].model_dump() for d in data])
        sql_insert = sql_insert.prefix_with("IGNORE")

        sql_select = select(table)
        sql_insert = sql_select.where

        async with await self._get_session() as session:
            session: AsyncSession
            # insert scraperdata
            await session.execute(sql_insert)

            # get scraper_id
            for d in data:
                _d: ScraperCreate = d["record"]
                date = datetime.fromisoformat(_d.created_at).date()
                sql_select = select(table)
                sql_select = sql_select.where(
                    and_(table.player_id == _d.player_id, table.record_date == date)
                )
                result = await session.execute(sql_select)
                result = result.first()
                scraper_id = ""  # TODO from result
                SKILLS = []  # hardcoded
                ACTIVITIES = []  # hardcoded

                d["skills"] = [
                    {
                        "name": s,
                        "skill_value": d["highscore"].get(s),
                        "scraper_id": scraper_id,
                    }
                    for s in SKILLS
                ]
                d["activities"] = [
                    {
                        "name": a,
                        "activity_value": d["highscore"].get(a),
                        "scraper_id": scraper_id,
                    }
                    for a in ACTIVITIES
                ]
                d.pop("highscore")  # cleanup memory
            # TODO: bulk insert values with repo skills & activities
            # REPO skills & activities must convert skill/activity_name to id
            # REPO skills & activities must take a pydantic class so we should convert above to class name:str, skill/activity_value:int, scraper_id:int
            await session.commit()
        return data
