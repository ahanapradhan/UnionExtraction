import oracledb
from oracledb import OperationalError

from mysite.unmasque.refactored.util.oracle_queries import OracleQueries
from mysite.unmasque.src.core.abstract.abstractConnection import AbstractConnectionHelper
import oracledb as cx_Oracle

from mysite.unmasque.src.util.constants import OK


class OracleConnectionHelper(AbstractConnectionHelper):
    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self.paramString = f'{self.config.user}/{self.config.password}@{self.config.host}:{self.config.port}/{self.config.dbname}'
        self.queries = OracleQueries()
        self.config.config_loaded = True

    def test_connection(self):
        try:
            self.connectUsingParams()
        except OperationalError:
            return 'Invalid credentials. Please try again.'
        self.closeConnection()
        return OK

    def connectUsingParams(self):
        self.conn = cx_Oracle.connect(self.paramString)

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
        except oracledb.Error as e:
            if logger is not None:
                logger.error(e)
            des = str(e)
        return res, des

    def get_DictCursor(self):
        return None

    def cus_execute_sqls(self, cur, sqls, logger=None):
        # print(cur)
        for sql in sqls:
            # print("..cur execute.." + sql)
            try:
                if isinstance(sql, str):
                    cur.execute(sql)
                else:
                    func = getattr(self.queries, sql[0])
                    sql_q = func(*sql[1:])
                    cur.execute(sql_q)
            except oracledb.Error as e:
                if logger is not None:
                    logger.error(e)
            # print("..done")
        cur.close()

    def cur_execute_sql_fetch_one_0(self, cur, sql, logger=None):
        prev = None
        try:
            cur.execute(sql)
            prev = cur.fetchone()
            prev = prev[0]
            cur.close()
        except oracledb.Error as e:
            if logger is not None:
                logger.error(e)
        return prev

    def cur_execute_sql_fetch_one(self, cur, sql, logger=None):
        prev = None
        try:
            cur.execute(sql)
            prev = cur.fetchone()
            cur.close()
        except oracledb.Error as e:
            if logger is not None:
                logger.error(e)
        return prev
