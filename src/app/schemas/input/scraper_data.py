from typing import Optional

from pydantic import BaseModel, ConfigDict


class ScraperData(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    player_id: int
    scraper_id: Optional[int] = None
    created_at: Optional[str] = None
    record_date: Optional[str] = None
