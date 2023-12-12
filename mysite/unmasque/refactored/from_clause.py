from ..refactored.abstract.ExtractorBase import Base
from ..refactored.executable import Executable
from ..refactored.initialization import Initiator
from ..refactored.util.common_queries import alter_table_rename_to, create_table_like
from ..refactored.util.utils import isQ_result_empty
from ..src.util.constants import REL_ERROR

try:
    import psycopg2
except ImportError:
    pass


class FromClause(Base):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "FromClause")
        self.app = Executable(connectionHelper)
        self.init = Initiator(connectionHelper)

        self.all_relations = set()
        self.core_relations = []

    def get_core_relations_by_rename(self, query):
        for tabname in self.all_relations:
            try:
                self.connectionHelper.execute_sql(
                    ["BEGIN;", alter_table_rename_to(tabname, "temp"), create_table_like(tabname, "temp")])

                new_result = self.app.doJob(query)
                if isQ_result_empty(new_result):
                    self.core_relations.append(tabname)
                    self.logger.info("Table ", tabname, " is in from clause..")

            except Exception as error:
                self.logger.error("Error Occurred in table extraction. Error: " + str(error))
                self.connectionHelper.execute_sql(["ROLLBACK;"])
                # exit(1)

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
                if str(error) == REL_ERROR:
                    self.core_relations.append(tabname)
                else:
                    self.logger.error("Error Occurred in table extraction. Error: " + str(error))

            finally:
                self.connectionHelper.execute_sql(["ROLLBACK;"])

    def extract_params_from_args(self, args):
        return args[0], args[1]

    def doJob(self, *args):
        check = self.init.result
        if not self.init.done:
            check = self.init.doJob()
        if not check:
            return False
        self.all_relations = self.init.all_relations
        return super().doJob(*args)

    def doActualJob(self, args):
        self.connectionHelper.execute_sql(["set statement_timeout to '2s';"])
        query, method = self.extract_params_from_args(args)
        self.core_relations = []
        if method == "rename":
            self.get_core_relations_by_rename(query)
        else:
            self.get_core_relations_by_error(query)
        self.connectionHelper.execute_sql(["set statement_timeout to DEFAULT;"])
        return self.core_relations

    def get_key_lists(self):
        return self.init.global_key_lists


