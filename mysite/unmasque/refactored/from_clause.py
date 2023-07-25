from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.initialization import Initiator
from mysite.unmasque.refactored.util.common_queries import drop_table, alter_table_rename_to, create_table_like
from mysite.unmasque.refactored.util.utils import isQ_result_empty

try:
    import psycopg2
except ImportError:
    pass


class FromClause(Base):
    DEBUG_QUERY = "select pid, state, query from pg_stat_activity where datname = 'tpch';"
    TERMINATE_STUCK_QUERIES = "SELECT pg_terminate_backend(pid);"

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "FromClause")
        self.all_relations = set()
        self.core_relations = []
        self.app = Executable(connectionHelper)
        self.init = Initiator(connectionHelper)

    def get_core_relations_by_rename(self, query):
        for tabname in self.all_relations:
            try:
                self.connectionHelper.execute_sql(
                    ["BEGIN;", alter_table_rename_to(tabname, "temp"), create_table_like(tabname, "temp")])

                new_result = self.app.doJob(query)
                if isQ_result_empty(new_result):
                    self.core_relations.append(tabname)

            except Exception as error:
                print("Error Occurred in table extraction. Error: " + str(error))
                exit(1)
            finally:
                self.connectionHelper.execute_sql(["ROLLBACK;"])

    def get_core_relations_by_error(self, query):
        for tabname in self.all_relations:
            try:
                self.connectionHelper.execute_sql(["BEGIN;", alter_table_rename_to(tabname, "temp")])

                try:
                    new_result = self.app.doJob(query)  # slow
                    if isQ_result_empty(new_result):
                        self.core_relations.append(tabname)
                except psycopg2.Error as e:
                    if e.pgcode == '42P01':
                        self.core_relations.append(tabname)
                    elif e.pgcode != '57014':
                        raise

            except Exception as error:
                print("Error Occurred in table extraction. Error: " + str(error))

            finally:
                self.connectionHelper.execute_sql(["ROLLBACK;"])

    def extract_params_from_args(self, args):
        return args[0][0], args[0][1]

    def doJob(self, *args):
        check = self.init.result
        if not self.init.done:
            check = self.init.doJob()
        if not check:
            return False
        self.all_relations = self.init.all_relations
        return super().doJob(*args)

    def doActualJob(self, args):
        query, method = self.extract_params_from_args(args)
        self.core_relations = []
        if method == "rename":
            self.get_core_relations_by_rename(query)
        else:
            self.get_core_relations_by_error(query)
        return self.core_relations


def getCoreRelations(connHelper, *args):
    fc = FromClause(connHelper)
    return fc.doJob(args)
