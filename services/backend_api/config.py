import os
from services.db_config import DB_PORT, DB_USER, DB_PASS, DB_HOST, DB_NAME_2
from services.config import REDIS_HOST, REDIS_PORT, REDIS_DB


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey")

    # PostgreSQL Connection
    DB_NAME = DB_NAME_2
    DB_USER = DB_USER
    DB_PASSWORD = DB_PASS
    DB_HOST = DB_HOST
    DB_PORT = DB_PORT

    SQLALCHEMY_DATABASE_URI = (
        f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    REDIS_HOST = REDIS_HOST
    REDIS_PORT = REDIS_PORT
    REDIS_PASSWORD = REDIS_DB
