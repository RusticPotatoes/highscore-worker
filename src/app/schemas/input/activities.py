from pydantic import BaseModel, ConfigDict
from typing import Optional

class Activities(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    activity_id: int
    activity_name: str


class PlayerActivities(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    scraper_id: Optional[int] = None
    activity_id: int
    activity_value: int
