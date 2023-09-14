import psycopg2
import psycopg2.extras

from .configParser import Config
from .constants import DBNAME, HOST, PORT, USER, PASSWORD, SCHEMA


def cus_execute_sqls(cur, sqls):
    print(cur)
    for sql in sqls:
        print("..cur execute.." + sql)
        cur.execute(sql)
        print("..done")
    cur.close()


def cus_execute_sql_with_params(cur, sql, params):
    for param in params:
        cur.execute(sql, param)
    cur.close()


def cur_execute_sql_fetch_one_0(cur, sql):
    cur.execute(sql)
    prev = cur.fetchone()
    prev = prev[0]
    cur.close()
    return prev


def cur_execute_sql_fetch_one(cur, sql):
    cur.execute(sql)
    prev = cur.fetchone()
    cur.close()
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

        self.conn = None
        self.db = self.config.dbname
        self.paramString = "dbname=" + self.config.dbname + " user=" + self.config.user + \
                           " password=" + self.config.password + " host=" + self.config.host + " port=" + self.config.port

    def closeConnection(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def connectUsingParams(self):
        self.conn = psycopg2.connect(self.paramString)

    def getConnection(self):
        if self.conn is None:
            print("connecting...")
            self.connectUsingParams()
            print("done!")
        return self.conn

    def execute_sql(self, sqls):
        cur = self.get_cursor()
        cus_execute_sqls(cur, sqls)

    def execute_sql_with_params(self, sql, params):
        cur = self.get_cursor()
        cus_execute_sql_with_params(cur, sql, params)

    def execute_sqls_with_DictCursor(self, sqls):
        cur = self.get_DictCursor()
        cus_execute_sqls(cur, sqls)

    def execute_sql_fetchone_0(self, sql):
        cur = self.get_cursor()
        return cur_execute_sql_fetch_one_0(cur, sql)

    def execute_sql_fetchone(self, sql):
        cur = self.get_cursor()
        return cur_execute_sql_fetch_one(cur, sql)

    def execute_sql_with_DictCursor_fetchone_0(self, sql):
        cur = self.get_DictCursor()
        return cur_execute_sql_fetch_one_0(cur, sql)

    def execute_sql_fetchall(self, sql):
        cur = self.get_cursor()
        print("...", sql, "...")
        cur.execute(sql)
        res = cur.fetchall()
        des = cur.description
        cur.close()
        return res, des

    def get_cursor(self):
        return self.conn.cursor()

    def get_DictCursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
