import configparser
from pathlib import Path

from .application_type import ApplicationType
from .constants import DATABASE_SECTION, DETECT_HAVING, HOST, PORT, USER, PASSWORD, SCHEMA, DBNAME, \
    SUPPORT_SECTION, LEVEL, LOGGING_SECTION, FEATURE_SECTION, DETECT_UNION, DETECT_NEP, USE_CS2, DATABASE, DETECT_OR, \
    DETECT_OJ


class Config:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # default values
        self.database = "postgres"
        self.index_maker = "create_indexes.sql"
        self.pkfk = "pkfkrelations.csv"
        self.schema = "public"
        self.dbname = "tpch"
        self.port = "5432"
        self.password = "postgres"
        self.user = "postgres"
        self.host = "localhost"
        self.log_level = 'INFO'
        self.base_path = Path(__file__).parent.parent.parent.parent
        self.config_loaded = False
        self.detect_union = False
        self.detect_nep = False
        self.detect_or = False
        self.detect_oj = False
        self.use_cs2 = False
        self.detect_having = False
        self.app_type = ApplicationType.SQL_ERR_FWD
        self.database = "postgres"

    def parse_config(self):
        if self.config_loaded:
            return

        try:
            config_file = (self.base_path / "config.ini").resolve()
            config_object = configparser.ConfigParser()
            with open(config_file, "r") as file_object:
                config_object.read_file(file_object)
                self.database = config_object.get(DATABASE_SECTION, DATABASE)
                self.host = config_object.get(DATABASE_SECTION, HOST)
                self.port = config_object.get(DATABASE_SECTION, PORT)
                self.user = config_object.get(DATABASE_SECTION, USER)
                self.password = config_object.get(DATABASE_SECTION, PASSWORD)
                self.dbname = config_object.get(DATABASE_SECTION, DBNAME)
                self.schema = config_object.get(DATABASE_SECTION, SCHEMA)

                self.pkfk = config_object.get(SUPPORT_SECTION, "pkfk")
                self.index_maker = config_object.get(SUPPORT_SECTION, "index_maker")

                self.log_level = config_object.get(LOGGING_SECTION, LEVEL)

                self.config_optional_features(config_object)

        except FileNotFoundError:
            print("config.ini not found. Default configs loaded!")
        except KeyError:
            print(" config not found. Using default config!")

        self.config_loaded = True

    def config_optional_features(self, config_object):
        detect_union = config_object.get(FEATURE_SECTION, DETECT_UNION)
        if detect_union.lower() == "no":
            self.detect_union = False
        elif detect_union.lower() == "yes":
            self.detect_union = True
        detect_nep = config_object.get(FEATURE_SECTION, DETECT_NEP)
        if detect_nep.lower() == "no":
            self.detect_nep = False
        elif detect_nep.lower() == "yes":
            self.detect_nep = True
        use_cs2 = config_object.get(FEATURE_SECTION, USE_CS2)
        if use_cs2.lower() == "no":
            self.use_cs2 = False
        elif use_cs2.lower() == "yes":
            self.use_cs2 = True
        detect_or = config_object.get(FEATURE_SECTION, DETECT_OR)
        if detect_or.lower() == "no":
            self.detect_or = False
        elif detect_or.lower() == "yes":
            self.detect_or = True
        detect_oj = config_object.get(FEATURE_SECTION, DETECT_OJ)
        if detect_oj.lower() == "no":
            self.detect_oj = False
        elif detect_oj.lower() == "yes":
            self.detect_oj = True
            # self.detect_union = False
            self.detect_union = False
        detect_having = config_object.get(FEATURE_SECTION, DETECT_HAVING)
        if detect_having.lower() == "no":
            self.detect_having = False
        elif detect_having.lower() == "yes":
            print("Warining: Using Having pipeline. All other features [Union, NEP, OR, OJ] are disabled.")
            self.detect_union = False
            self.detect_nep = False
            self.detect_or = False
            self.detect_oj = False
            self.detect_having = True
