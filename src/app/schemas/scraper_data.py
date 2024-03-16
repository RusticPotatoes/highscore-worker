from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class ScraperCreate(BaseModel):
    player_id: int
    created_at: datetime


class ScraperData(ScraperCreate):
    model_config = ConfigDict(from_attributes=True)

    # ScraperCreate.player_id
    # ScraperCreate.created_at
    scraper_id: int
    record_date: date
