import oracledb
from oracledb import OperationalError

from mysite.unmasque.src.util.oracle_queries import OracleQueries
from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
import oracledb as cx_Oracle

from ...src.util.constants import OK


class OracleConnectionHelper(AbstractConnectionHelper):

    def rollback_transaction(self):
        self.execute_sql(["ROLLBACK"])

    def set_timeout_to_2s(self):
        pass

    def reset_timeout(self):
        pass

    def get_all_tables_for_restore(self):
        pass

    def is_view_or_table(self, tab):
        pass

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self.paramString = f'{self.config.user}/{self.config.password}@{self.config.host}:{self.config.port}/{self.config.dbname}'
        self.queries = OracleQueries()
        self.config.config_loaded = True
        self.queries.set_schema(self.config.schema)

    def form_query(self, selections, wheres):
        pass

    def begin_transaction(self):
        self.execute_sql(["BEGIN;"])

    def commit_transaction(self):
        self.execute_sql(["COMMIT;"])

    def test_connection(self):
        try:
            self.connectUsingParams()
        except OperationalError:
            return 'Invalid credentials. Please try again.'
        self.closeConnection()
        return OK

    def connectUsingParams(self):
        self.conn = cx_Oracle.connect(self.paramString)

    def cus_execute_sql_with_params(self, cur, sql, params, logger=None):
        for param in params:
            query = f"{sql} {str(param)}"
            if logger is not None:
                logger.debug(query)
            cur.execute(query)
        cur.close()

    def execute_sql_fetchall(self, sql, logger=None):
        res = None
        des = None
        cur = self.get_cursor()
        try:
            cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.config.user}")
            cur.execute(sql)
            res = cur.fetchall()
            des = cur.description
            cur.close()
        except oracledb.Error as e:
            if logger is not None:
                logger.error(e)
            print(sql)
            des = str(e)
            print(des)
            raise ValueError(des)
        return res, des

    def get_DictCursor(self):
        return None

    def cus_execute_sqls(self, cur, sqls, logger=None):
        cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.config.user}")
        # print(cur)
        for sql in sqls:
            if logger is not None:
                logger.debug(f"..cur execute..{sql}")
            try:
                cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.config.user}")
                cur.execute(sql)
                # cur.execute(f"COMMIT")
            except oracledb.Error as e:
                if logger is not None:
                    logger.error(e)
                print(sql)
                print(e)
                raise ValueError
            # print("..done")
        cur.close()

    def cur_execute_sql_fetch_one_0(self, cur, sql, logger=None):
        cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.config.user}")
        prev = None
        try:
            cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.config.user}")
            cur.execute(sql)
            prev = cur.fetchone()
            prev = prev[0]
            cur.close()
        except oracledb.Error as e:
            if logger is not None:
                logger.error(e)
        return prev

    def cur_execute_sql_fetch_one(self, cur, sql, logger=None):
        cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.config.user}")
        prev = None
        try:
            cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.config.user}")
            cur.execute(sql)
            prev = cur.fetchone()
            cur.close()
        except oracledb.Error as e:
            if logger is not None:
                logger.error(e)
        return prev
