import psycopg2
import psycopg2.extras
import oracledb as cx_Oracle

from .configParser import Config
from .constants import DBNAME, HOST, PORT, USER, PASSWORD, SCHEMA, DATABASE


def cus_execute_sqls(cur, sqls, logger=None):
    for sql in sqls:
        if logger is not None:
            logger.debug(f"..cur execute..{sql}")
        try:
            cur.execute(sql)
        except psycopg2.ProgrammingError as e:
            if logger is not None:
                logger.error(e)
                logger.error(e.diag.message_detail)
        except cx_Oracle.Error as e:
            if logger is not None:
                logger.error(e)
    cur.close()


def cus_execute_sql_with_params(cur, sql, params, logger=None):
    for param in params:
        if logger is not None:
            logger.debug(sql, param)
        cur.execute(sql, param)
        if logger is not None:
            logger.debug(sql, param)
    cur.close()


def cur_execute_sql_fetch_one_0(cur, sql, logger=None):
    prev = None
    try:
        cur.execute(sql)
        prev = cur.fetchone()
        prev = prev[0]
        cur.close()
    except psycopg2.ProgrammingError as e:
        if logger is not None:
            logger.error(e)
            logger.error(e.diag.message_detail)
    return prev


def cur_execute_sql_fetch_one(cur, sql, logger=None):
    prev = None
    try:
        cur.execute(sql)
        prev = cur.fetchone()
        cur.close()
    except psycopg2.ProgrammingError as e:
        if logger is not None:
            logger.error(e)
            logger.error(e.diag.message_detail)
    return prev


class ConnectionHelper:

    def __init__(self, **kwargs):
        """
        Default configs are loaded first
        """
        self.config = Config()

        """
        If config.ini available in the backend, prioritize it
        """
        self.config.parse_config()

        """
        If configs come from the caller (e.g. UI), prioritize it
        """
        for key, value in kwargs.items():
            if key == DATABASE:
                self.config.database = value
            if key == DBNAME:
                self.config.dbname = value
            elif key == HOST:
                self.config.host = value
            elif key == PORT:
                self.config.port = value
            elif key == USER:
                self.config.user = value
            elif key == PASSWORD:
                self.config.password = value
            elif key == SCHEMA:
                self.config.schema = value

        self.config.config_loaded = True

        self.database = self.config.database

        self.conn = None
        self.db = self.config.dbname
        self.paramString = f"dbname={self.config.dbname} user={self.config.user} password={self.config.password} " \
                           f"host={self.config.host} port={self.config.port}"
        self.dsn = f'{self.config.user}/{self.config.password}@{self.config.host}:{self.config.port}/{self.config.dbname}'

    def closeConnection(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def connectUsingParams(self):
        if self.database == "postgres":
            self.conn = psycopg2.connect(self.paramString)
        elif self.database == "oracle":
            self.conn = cx_Oracle.connect(self.dsn)

    def getConnection(self):
        if self.conn is None:
            self.connectUsingParams()
        return self.conn

    def execute_sql(self, sqls, logger=None):
        cur = self.get_cursor()
        cus_execute_sqls(cur, sqls, logger)

    def execute_sql_with_params(self, sql, params, logger=None):
        cur = self.get_cursor()
        cus_execute_sql_with_params(cur, sql, params, logger)

    def execute_sqls_with_DictCursor(self, sqls, logger=None):
        cur = self.get_DictCursor()
        cus_execute_sqls(cur, sqls, logger)

    def execute_sql_fetchone_0(self, sql, logger=None):
        cur = self.get_cursor()
        return cur_execute_sql_fetch_one_0(cur, sql, logger)

    def execute_sql_fetchone(self, sql, logger=None):
        cur = self.get_cursor()
        return cur_execute_sql_fetch_one(cur, sql, logger)

    def execute_sql_with_DictCursor_fetchone_0(self, sql, logger=None):
        cur = self.get_DictCursor()
        return cur_execute_sql_fetch_one_0(cur, sql, logger)

    def execute_sql_fetchall(self, sql, logger=None):
        res = None
        des = None
        cur = self.get_cursor()
        # print("...", sql, "...")
        try:
            cur.execute(sql)
            res = cur.fetchall()
            des = cur.description
            cur.close()
        except psycopg2.ProgrammingError as e:
            if logger is not None:
                logger.error(e)
                logger.error(e.diag.message_detail)
            des = str(e)
        except cx_Oracle.Error as e:
            if logger is not None:
                logger.error(e)
            des = str(e)
        return res, des

    def get_cursor(self):
        cur = self.conn.cursor()
        return cur

    def get_DictCursor(self):
        if self.database == "postgres":
            return self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
