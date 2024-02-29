from typing import Optional

from pydantic import BaseModel, ConfigDict
from datetime import datetime

class ScraperData(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    player_id: int
    scraper_id: Optional[int] = None
    created_at: str = datetime.now().isoformat() #datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # record_date: Optional[str] = None
