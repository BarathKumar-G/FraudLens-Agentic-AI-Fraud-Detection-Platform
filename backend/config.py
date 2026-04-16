import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    s3_bucket: str
    environment: str = "development"

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), "../.env")
        extra = "ignore"

settings = Settings()