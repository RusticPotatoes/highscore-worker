from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    KAFKA_HOST: str


settings = Settings()
