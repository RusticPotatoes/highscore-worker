from app.schemas.input.highscore import PlayerHiscoreData
from app.schemas.input.player import Player
from pydantic import BaseModel


class Message(BaseModel):
    hiscores: PlayerHiscoreData | None = None
    player: Player
