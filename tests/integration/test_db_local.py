import pytest
from sqlalchemy import text
from src.config.database import get_engine


@pytest.mark.integration
def test_postgres_connection():
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))

    version = result.scalar()

    assert "PostgreSQL" in version
