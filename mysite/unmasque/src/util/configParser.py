import configparser
from pathlib import Path

from .constants import DATABASE_SECTION, HOST, PORT, USER, PASSWORD, SCHEMA, DBNAME, \
    SUPPORT_SECTION, LEVEL, LOGGING_SECTION


class Config:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # default values
        self.index_maker = "create_indexes.sql"
        self.pkfk = "pkfkrelations.csv"
        self.schema = "public"
        self.dbname = "tpch"
        self.port = "5432"
        self.password = "postgres"
        self.user = "postgres"
        self.host = "localhost"
        self.log_level = 'INFO'
        self.base_path = None
        self.config_loaded = False
        self.detect_union = False

    def parse_config(self):
        if self.config_loaded:
            return

        try:
            self.base_path = Path(__file__).parent.parent.parent.parent
            config_file = (self.base_path / "config.ini").resolve()
            config_object = configparser.ConfigParser()
            with open(config_file, "r") as file_object:
                config_object.read_file(file_object)
                self.host = config_object.get(DATABASE_SECTION, HOST)
                self.port = config_object.get(DATABASE_SECTION, PORT)
                self.user = config_object.get(DATABASE_SECTION, USER)
                self.password = config_object.get(DATABASE_SECTION, PASSWORD)
                self.dbname = config_object.get(DATABASE_SECTION, DBNAME)
                self.schema = config_object.get(DATABASE_SECTION, SCHEMA)

                self.pkfk = config_object.get(SUPPORT_SECTION, "pkfk")
                self.index_maker = config_object.get(SUPPORT_SECTION, "index_maker")

                self.log_level = config_object.get(LOGGING_SECTION, LEVEL)
        except FileNotFoundError:
            print("config.ini not found. Default configs loaded!")
        except KeyError:
            print(" config not found. Using default config!")

        self.config_loaded = True
