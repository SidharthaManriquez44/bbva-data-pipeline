import subprocess
import time
import pytest
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pathlib

load_dotenv(pathlib.Path(__file__).parent / ".env.test")


def wait_for_postgres():
    engine = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/airflow")

    for i in range(30):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            print("Postgres is ready")
            return

        except Exception as e:
            print(f"Attempt {i+1}: {e}")
            time.sleep(2)

    raise RuntimeError("Postgres container did not become ready in time")


@pytest.fixture(scope="session", autouse=True)
def postgres_container():
    subprocess.run(["docker", "compose", "up", "-d", "postgres"], check=True)

    wait_for_postgres()

    yield

    subprocess.run(["docker", "compose", "stop", "postgres"], check=True)
