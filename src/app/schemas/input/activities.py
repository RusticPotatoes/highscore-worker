from pydantic import BaseModel, ConfigDict


class Activities(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    activity_id: int
    activity_name: str


class PlayerActivities(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    scraper_id: int
    activity_id: int
    activity_value: int
