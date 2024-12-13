import re
from _decimal import Decimal

import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError

from .constants import OK
from ..core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.util.postgres_queries import PostgresQueries


class PostgresConnectionHelper(AbstractConnectionHelper):

    def get_all_tables_for_restore(self):
        pass

    def rollback_transaction(self):
        self.execute_sql(["ROLLBACK;"])

    def set_timeout_to_2s(self):
        return "set statement_timeout to '2s';"

    def reset_timeout(self):
        return "set statement_timeout to DEFAULT;"

    #def get_all_tables_for_restore(self):
    #    res, desc = self.execute_sql_fetchall(
    #        self.get_sanitization_select_query(["SPLIT_PART(table_name, '_', 1) as original_name"],
    #                                           ["table_name like '%_backup'"]))
    #    tables = [row[0] for row in res]
    #    print(tables)
    #    return tables

    def is_view_or_table(self, tab, schema):
        # Reference: https://www.postgresql.org/docs/current/infoschema-tables.html
        check_query = self.get_sanitization_select_query(["table_type"], [f" table_name = '{tab}'"], schema)
        res, _ = self.execute_sql_fetchall(check_query)

        if len(res) > 0:
            if res[0][0] == 'VIEW':
                return 'view'
            else:
                return 'table'

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self.paramString = f"dbname={self.config.dbname} user={self.config.user} password={self.config.password} " \
                           f"host={self.config.host} port={self.config.port}"
        self.queries = PostgresQueries()
        self.config.config_loaded = True

    def form_query(self, selections, wheres, schema):
        query = f"Select {selections}  From information_schema.tables " + \
                f"WHERE table_schema = '{schema}' and " \
                f"TABLE_CATALOG= '{self.config.dbname}' {wheres} ;"
        query = re.sub(' +', ' ', query)
        return query

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

    def connectUsingParams(self, single_worker=False):
        self.conn = psycopg2.connect(self.paramString)
        with self.conn.cursor() as set_cur:
            if single_worker:
                set_cur.execute("SET max_parallel_workers_per_gather = 0;")
            if self.config.workmem is not None:
                set_cur.execute(f"SET work_mem = '{self.config.workmem}';")

    def cus_execute_sql_with_params(self, cur, sql, params, logger=None):
        for param in params:
            if logger is not None:
                logger.debug(sql, param)
            try:
                cur.execute(sql, param)
            except Exception as e:
                if logger is not None:
                    logger.error(str(e))
            # finally:
            #    cur.close()

    def execute_sql_fetchall(self, sql, logger=None):
        cur = self.get_cursor()
        if logger is not None:
            logger.debug("..cur execute.." + sql)
        try:
            cur.execute(sql)
            res = cur.fetchall()
            des = cur.description
        except psycopg2.ProgrammingError as e:
            if logger is not None:
                logger.error(e)
                logger.error(e.diag.message_detail)
            des = str(e)
            raise ValueError(des)
        # finally:
        #    cur.close()
        return res, des

    def get_DictCursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def cus_execute_sqls(self, cur, sqls, logger=None):
        # print(cur)
        for sql in sqls:
            if logger is not None:
               logger.debug("..cur execute.." + sql)
            try:
                cur.execute(sql)
            except psycopg2.ProgrammingError as e:
                #print(e)
                if logger is not None:
                    logger.error(e)
                    logger.error(e.diag.message_detail)
            # print("..done")
            except ValueError as e:
                raise e
            # finally:
            #    cur.close()

    def cur_execute_sql_fetch_one_0(self, cur, sql, logger=None):
        prev = None
        if logger is not None:
            logger.debug(sql)
        try:
            cur.execute(sql)
            prev = cur.fetchone()
            prev = prev[0]
            if isinstance(prev, Decimal):
                prev = float(prev)
        except psycopg2.ProgrammingError as e:
            if logger is not None:
                logger.error(e)
                logger.error(e.diag.message_detail)
        # finally:
        #    cur.close()
        return prev

    def cur_execute_sql_fetch_one(self, cur, sql, logger=None):
        prev = None
        # if logger is not None:
        #    logger.debug("..cur execute.." + sql)
        try:
            cur.execute(sql)
            prev = cur.fetchone()
        except psycopg2.ProgrammingError as e:
            if logger is not None:
                logger.error(e)
                logger.error(e.diag.message_detail)
        # finally:
        #    cur.close()
        return prev
