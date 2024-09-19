import pandas as pd

from ..executables.ZeroResultExecutable import ZeroResultExecutable
from ..executables.nullfree_executable import NullFreeExecutable


class ExecutableFactory:
    _instance = None
    app = None
    query = None

    # For maintaining a singleton class
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ExecutableFactory, cls).__new__(cls)
        return cls._instance

    def set_hidden_query(self, query):
        self.query = query

    def create_exe(self, connectionHelper):
        check = connectionHelper.config.detect_oj
        if check and self.query is not None:
            connectionHelper.connectUsingParams()
            sql_query = pd.read_sql_query(self.query, connectionHelper.conn)
            df = pd.DataFrame(sql_query)
            check = df.isnull().values.any()
            connectionHelper.closeConnection()
        if check:
            obj = NullFreeExecutable(connectionHelper)
        else:
            obj = ZeroResultExecutable(connectionHelper)  # to handle pure count queries # Executable(connectionHelper)
        self.app = obj
        return obj
