import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer


@pytest.mark.integration
def test_postgres_connection():
    with PostgresContainer("postgres:16") as postgres:
        engine = create_engine(postgres.get_connection_url())

        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))

        version = result.scalar()

        assert "PostgreSQL" in version
