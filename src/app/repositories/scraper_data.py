from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.repositories.abc import ABCRepo
from src.app.schemas.input.scraper_data import ScraperData
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

    async def create(self, data: list[ScraperData]) -> None:
        table = ScraperDataDB
        sql = insert(table)
        sql = sql.values([d.model_dump() for d in data])
        sql = sql.prefix_with("IGNORE")

        async with await self._get_session() as session:
            session: AsyncSession
            data = await session.execute(sql)
        return
