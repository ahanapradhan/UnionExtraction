from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.util.Log import Log
from ...test.util import tpchSettings


class TPCHRestore:
    user_schema = "public"
    backup_schema = "tpch_restore"

    def __init__(self, conn: AbstractConnectionHelper):
        self.conn = conn
        self.relations = tpchSettings.relations
        self.logger = Log("Test Schema Restore", conn.config.log_level)

    def doJob(self):
        return
        self.conn.connectUsingParams()
        for tab in self.relations:
            self.conn.execute_sql(
                [f"truncate {self.user_schema}.{tab};",
                 f"insert into {self.user_schema}.{tab} select * from {self.backup_schema}.{tab};",
                 f"ALTER TABLE {self.user_schema}.{tab} SET (autovacuum_enabled = false);"
                 "commit;"], self.logger)
        self.conn.closeConnection()
