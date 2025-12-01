from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # These attributes will automatically look for matching keys
    # in the environment (e.g., os.environ) and the .env file.
    endpoint_url: str = Field(..., env='ENDPOINT_URL')
    s3_username: str = Field(..., env='S3_USERNAME')
    s3_password: str = Field(..., env='S3_PASSWORD')

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings()