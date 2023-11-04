from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    KAFKA_HOST: str
    DATABASE_URL: str
    POOL_TIMEOUT: int
    POOL_RECYCLE: int
    ENV: str = "DEV"


settings = Settings()
