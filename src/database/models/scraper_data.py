from database.database import Base
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    func,
)
from sqlalchemy.dialects.mysql import BIGINT, SMALLINT


class ScraperData(Base):
    __tablename__ = "scraper_data"

    scraper_id = Column(BIGINT, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    player_id = Column(SMALLINT, nullable=False)
    record_date = Column(Date, nullable=True)
