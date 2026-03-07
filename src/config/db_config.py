import os
from sqlalchemy import create_engine


def get_engine():
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    db = os.environ["DB_NAME"]

    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_size=5,
        max_overflow=10
    )