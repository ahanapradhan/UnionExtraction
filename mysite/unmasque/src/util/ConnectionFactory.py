from .PostgresConnectionHelper import PostgresConnectionHelper
from .Oracle_connectionHelper import OracleConnectionHelper
from .configParser import Config


class ConnectionHelperFactory:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ConnectionHelperFactory, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):

        """
        Default configs are loaded first
        """
        self.config = Config()

        """
        If config.ini available in the backend, prioritize it
        """
        self.config.parse_config()

    def createConnectionHelper(self, config=None, **kwargs):
        self.config = Config() if config is None else config
        if self.config.database == "postgres":
            return PostgresConnectionHelper(self.config, **kwargs)
        elif self.config.database == "oracle":
            return OracleConnectionHelper(self.config, **kwargs)
