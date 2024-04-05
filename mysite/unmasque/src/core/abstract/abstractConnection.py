from abc import abstractmethod

from ....src.util.constants import DBNAME, HOST, PORT, USER, PASSWORD, SCHEMA, OK


class AbstractConnectionHelper:
    def __init__(self, config=None, **kwargs):
        self.config = config
        if self.config is not None:
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

        self.conn = None
        self.queries = None

    @abstractmethod
    def set_timeout_to_2s(self):
        pass

    @abstractmethod
    def reset_timeout(self):
        pass

    @abstractmethod
    def begin_transaction(self):
        pass

    @abstractmethod
    def commit_transaction(self):
        pass

    @abstractmethod
    def rollback_transaction(self):
        pass

    def get_sanitization_select_query(self, projectns, predicates):
        selections = " and ".join(projectns)
        wheres = " and ".join(predicates)
        if len(predicates) == 1:
            wheres = " and " + wheres
        return self.form_query(selections, wheres)

    @abstractmethod
    def test_connection(self):
        pass

    def validate_query(self, sql):
        try:
            self.connectUsingParams()
            self.execute_sql_fetchall(self.queries.get_explain_query(sql))
            self.closeConnection()
        except Exception as e:
            return str(e)
        return OK

    def closeConnection(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    @abstractmethod
    def connectUsingParams(self):
        pass

    def getConnection(self):
        if self.conn is None:
            self.connectUsingParams()
        return self.conn

    def execute_sql(self, sqls, logger=None):
        cur = self.get_cursor()
        self.cus_execute_sqls(cur, sqls, logger)

    def execute_sql_with_params(self, sql, params, logger=None):
        cur = self.get_cursor()
        self.cus_execute_sql_with_params(cur, sql, params, logger)

    def execute_sqls_with_DictCursor(self, sqls, logger=None):
        cur = self.get_DictCursor()
        self.cus_execute_sqls(cur, sqls, logger)

    def execute_sql_fetchone_0(self, sql, logger=None):
        cur = self.get_cursor()
        return self.cur_execute_sql_fetch_one_0(cur, sql, logger)

    def execute_sql_fetchone(self, sql, logger=None):
        cur = self.get_cursor()
        return self.cur_execute_sql_fetch_one(cur, sql, logger)

    def execute_sql_with_DictCursor_fetchone_0(self, sql, logger=None):
        cur = self.get_DictCursor()
        return self.cur_execute_sql_fetch_one_0(cur, sql, logger)

    @abstractmethod
    def execute_sql_fetchall(self, sql, logger=None):
        pass

    def get_cursor(self):
        cur = self.conn.cursor()
        return cur

    @abstractmethod
    def get_DictCursor(self):
        pass

    @abstractmethod
    def cus_execute_sqls(self, cur, sqls, logger=None):
        pass

    @abstractmethod
    def cus_execute_sql_with_params(self, cur, sql, params, logger=None):
        pass

    @abstractmethod
    def cur_execute_sql_fetch_one_0(self, cur, sql, logger=None):
        pass

    @abstractmethod
    def cur_execute_sql_fetch_one(self, cur, sql, logger=None):
        pass

    @abstractmethod
    def form_query(self, selections, wheres):
        pass

    @abstractmethod
    def is_view_or_table(self, tab):
        pass

    @abstractmethod
    def get_all_tables_for_restore(self):
        pass
