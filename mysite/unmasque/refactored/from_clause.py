from .abstract.AppExtractorBase import AppExtractorBase
from ..refactored.initialization import Initiator
from ..refactored.util.common_queries import alter_table_rename_to, create_table_like
from ..refactored.util.utils import isQ_result_empty
from ..src.util.application_type import ApplicationType
from ..src.util.constants import REL_ERROR

try:
    import psycopg2
except ImportError:
    pass


class FromClause(AppExtractorBase):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "FromClause")
        self.init = Initiator(connectionHelper)

        self.all_relations = set()
        self.core_relations = []
        self.method = "error"
        self.timeout = True

    def set_app_type(self):
        app_type = self.connectionHelper.config.app_type
        if app_type == ApplicationType.SQL_ERR_FWD:
            self.timeout = True
            self.method = "error"
        elif app_type == ApplicationType.SQL_NO_ERR_FWD:
            self.method = "rename"
        elif app_type == ApplicationType.IMPERATIVE_ERR_FWD:
            self.timeout = False
            self.method = "error"
        elif app_type == ApplicationType.IMPERATIVE_NO_ERR_FWD:
            self.method = "rename"

    def get_core_relations_by_rename(self, query):
        for tabname in self.all_relations:
            try:
                self.connectionHelper.execute_sql(
                    ["BEGIN;", alter_table_rename_to(tabname, "temp"), create_table_like(tabname, "temp")], self.logger)

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
                self.connectionHelper.execute_sql(["BEGIN;", alter_table_rename_to(tabname, "temp")], self.logger)

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
        if len(args) == 1:
            return args[0], ""
        return args[0], args[1]

    def setup(self):
        self.set_app_type()
        check = self.init.result
        if not self.init.done:
            check = self.init.doJob()
        if not check:
            return False
        self.all_relations = self.init.all_relations
        return True

    def doActualJob(self, args):
        setup_done = self.setup()
        if not setup_done:
            return False

        if self.timeout:
            self.connectionHelper.execute_sql(["set statement_timeout to '2s';"])
        query, method = self.extract_params_from_args(args)
        if not method:
            method = self.method
        self.core_relations = []
        if method == "rename":
            self.get_core_relations_by_rename(query)
        else:
            self.get_core_relations_by_error(query)
        self.connectionHelper.execute_sql(["set statement_timeout to DEFAULT;"])
        return self.core_relations

    def get_key_lists(self):
        return self.init.global_key_lists
