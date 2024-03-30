import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError

from .constants import OK
from ..core.abstract.abstractConnection import AbstractConnectionHelper
from ...refactored.util.postgres_queries import PostgresQueries


class PostgresConnectionHelper(AbstractConnectionHelper):

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self.paramString = f"dbname={self.config.dbname} user={self.config.user} password={self.config.password} " \
                           f"host={self.config.host} port={self.config.port}"
        self.queries = PostgresQueries()
        self.config.config_loaded = True

    def test_connection(self):
        try:
            self.connectUsingParams()
        except OperationalError:
            return 'Invalid credentials. Please try again.'
        self.closeConnection()
        return OK

    def connectUsingParams(self):
        self.conn = psycopg2.connect(self.paramString)

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
        return res, des

    def get_DictCursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def cus_execute_sqls(self, cur, sqls, logger=None):
        # print(cur)
        for sql in sqls:
            # print("..cur execute.." + sql)
            try:
                if isinstance(sql, str):
                    print("..cur execute.." + sql)
                    cur.execute(sql)
                else:
                    func = getattr(self.queries, sql[0])
                    sql_q = func(*sql[1:])
                    print("..cur execute.." + sql_q)
                    cur.execute(sql_q)
            except psycopg2.ProgrammingError as e:
                if logger is not None:
                    logger.error(e)
                    logger.error(e.diag.message_detail)
            # print("..done")
        cur.close()

    def cur_execute_sql_fetch_one_0(self, cur, sql, logger=None):
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

    def cur_execute_sql_fetch_one(self, cur, sql, logger=None):
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
