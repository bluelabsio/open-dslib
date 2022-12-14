import os
from dataclasses import dataclass, field
from typing import Optional, Type
from types import TracebackType

import sqlalchemy
from sqlalchemy.engine import Connection
from urllib.parse import quote_plus as urlquote


@dataclass
class EngineContext:
    """sqlalchemy engine context manager. Pulls connection info from the environment.
    """

    name: str
    driver: str = field(default=None) #TO DO

    def __post_init__(self):
        try:
            user = os.environ[f"{self.name}_USER"]
            password = os.environ[f"{self.name}_PW"]
            host = os.environ[f"{self.name}_HOST"]
            dbname = os.environ[f"{self.name}_DB"]
            port = os.environ[f"{self.name}_PORT"]
            dialect = os.environ[f"{self.name}_TYPE"]
        except KeyError as key_error:
            raise KeyError(
                f"""Credentials associated with {self.name} not found!"""
            ) from key_error

        if self.driver is not None:
            self.engine_str = f"{dialect}+{self.driver}://{user}:{urlquote(password)}@{host}:{port}/{dbname}"
        else:
            self.engine_str = (
                f"{dialect}://{user}:{urlquote(password)}@{host}:{port}/{dbname}"
            )

        self.engine = sqlalchemy.create_engine(self.engine_str)
        self.con = None

    def create_connection(self) -> Connection:
        """Create a connection with instantiated credentials.
        Returns:
            Connection: a sqlalchemy connection to the database.
        """
        self.con = self.engine.connect()

        return self.con

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        if exc_tb is not None:
                self.con.rollback()  # issues with sqlalchemy-redshfit

        self.con.close()
