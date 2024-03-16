from typing import Optional

from pydantic import BaseModel, ConfigDict


class Player(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    possible_ban: Optional[bool] = None
    confirmed_ban: Optional[bool] = None
    confirmed_player: Optional[bool] = None
    label_id: Optional[int] = None
    label_jagex: Optional[int] = None
    ironman: Optional[bool] = None
    hardcore_ironman: Optional[bool] = None
    ultimate_ironman: Optional[bool] = None
    normalized_name: Optional[bool] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
