import os
from dotenv import load_dotenv

from pathlib import Path

env_path = Path('.env')
load_dotenv(dotenv_path=env_path)


class Settings:
    PROJECT_NAME: str = "GTFS-Viewer"
    PROJECT_VERSION: str = "1.0.0"

    USER_UID = 1
    ORIGINS = [
        "http://localhost:4200",
        "https://portfolio.amaury-valorge.com"
    ]
    API_PREFIX = "/api/v1/gtfs_builder"
    DATA_DIR = "data"

    # Environment variables
    AREAS: str = os.getenv("AREAS").split(",")
    ADMIN_DB_URL = os.getenv("ADMIN_DB_URL")
    MODE: str = os.getenv("MODE")
    DB_SCHEMA = "portfolio"


settings = Settings()
