from pydantic import BaseModel, ConfigDict


class Skills(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    skill_id: int
    skill_name: str


class PlayerSkills(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    scraper_id: int
    skill_id: int
    skill_value: int
