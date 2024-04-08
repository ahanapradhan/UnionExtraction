from ...src.core.abstract.abstractConnection import AbstractConnectionHelper


class TPCHRestore:
    user_schema = "public"
    backup_schema = "tpch_restore"

    def __init__(self, conn: AbstractConnectionHelper):
        self.conn = conn

    def doJob(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(["drop schema public cascade;",
                               "create schema public;",
                               "create table public.nation as select * from tpch_restore.nation;",
                               "create table public.region as select * from tpch_restore.region;",
                               "create table public.customer as select * from tpch_restore.customer;",
                               "create table public.lineitem as select * from tpch_restore.lineitem;",
                               "create table public.orders as select * from tpch_restore.orders;",
                               "create table public.part as select * from tpch_restore.part;",
                               "create table public.supplier as select * from tpch_restore.supplier;",
                               "create table public.partsupp as select * from tpch_restore.partsupp;",
                               "commit;"])
        self.conn.closeConnection()
