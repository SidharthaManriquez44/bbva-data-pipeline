import os


class Settings:
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "dev")

        self.db_name = os.getenv("DB_NAME")

        self._validate()

    def _validate(self):
        # valid environment
        if self.environment not in {"dev", "test", "prod"}:
            raise ValueError(f"Invalid ENVIRONMENT: {self.environment}")

        # critical protection
        if self.environment == "test":
            if self.db_name in {"bbva_dw", "production", "prod"}:
                raise ValueError("Tests are trying to use a non-test database")


settings = Settings()
