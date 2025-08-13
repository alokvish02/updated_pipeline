import os

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME_2 = "trading_system"
DB_USER = "postgres"
DB_PASS = "onealpha12345"
DB_SCHEMA = "public"

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey")
    DB_NAME = DB_NAME_2
    DB_USER = DB_USER
    DB_PASSWORD = DB_PASS
    DB_HOST = DB_HOST
    DB_PORT = DB_PORT
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

# Securely load Binance API credentials from environment variables
binance_api_key= "LYEu4haYVMKq6eLwZRL6abbaM3MYoHQnI9xGmF2NgIKG5QQIb0uFl3hrxiPTKext"
binance_api_secret= "UuXHuMrJ7mZiyDuQCNsbKOAA5y72uB6tXVAHYwa3iEKXvNzOIW6SkbvtMVUBvha1"
