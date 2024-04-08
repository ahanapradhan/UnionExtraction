from mysite.unmasque.src.core.abstract.abstractConnection import AbstractConnectionHelper


class TPCHRestore:
    user_schema = "public"
    backup_schema = "tpch_restore"

    def __init__(self, conn: AbstractConnectionHelper):
        self.conn = conn

    def doJob(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql([f"drop schema {self.user_schema} cascade;",
                               f"create schema {self.user_schema};",
                               f"create table {self.user_schema}.nation "
                               f"as select * from {self.backup_schema}.nation;",
                               f"create table {self.user_schema}.region "
                               f"as select * from {self.backup_schema}.region;",
                               f"create table {self.user_schema}.customer "
                               f"as select * from {self.backup_schema}.customer;",
                               f"create table {self.user_schema}.lineitem "
                               f"as select * from {self.backup_schema}.lineitem;",
                               f"create table {self.user_schema}.orders "
                               f"as select * from {self.backup_schema}.orders;",
                               f"create table {self.user_schema}.part "
                               f"as select * from {self.backup_schema}.part;",
                               f"create table {self.user_schema}.supplier "
                               f"as select * from {self.backup_schema}.supplier;",
                               f"create table {self.user_schema}.partsupp "
                               f"as select * from {self.backup_schema}.partsupp;",
                               "commit;"])
        self.conn.closeConnection()
