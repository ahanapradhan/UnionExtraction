from mysite.unmasque.src.core.abstract.AppExtractorBase import AppExtractorBase
from mysite.unmasque.src.core.initialization import Initiator
from mysite.unmasque.src.util.utils import isQ_result_empty
from mysite.unmasque.src.util.application_type import ApplicationType
from mysite.unmasque.src.util.constants import REL_ERROR


class FromClause(AppExtractorBase):
    TYPE_ERROR = "error"
    TYPE_RENAME = "rename"

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "FromClause")
        self.init = Initiator(connectionHelper)

        self.all_relations = set()
        self.core_relations = []
        self.method = self.TYPE_ERROR
        self.timeout = True

    def set_app_type(self):
        app_type = self.connectionHelper.config.app_type
        if app_type == ApplicationType.SQL_ERR_FWD:
            self.timeout = True
            self.method = self.TYPE_ERROR
        elif app_type == ApplicationType.SQL_NO_ERR_FWD:
            self.method = self.TYPE_RENAME
        elif app_type == ApplicationType.IMPERATIVE_ERR_FWD:
            self.timeout = False
            self.method = self.TYPE_ERROR
        elif app_type == ApplicationType.IMPERATIVE_NO_ERR_FWD:
            self.method = self.TYPE_RENAME

    def get_core_relations_by_rename(self, query):
        for tabname in self.all_relations:
            try:
                self.connectionHelper.begin_transaction()
                self.connectionHelper.execute_sql(
                    [self.connectionHelper.queries.alter_table_rename_to(tabname, "temp"),
                     self.connectionHelper.queries.create_table_like(tabname, "temp")], self.logger)
                new_result = self.app.doJob(query)
                if isQ_result_empty(new_result):
                    self.core_relations.append(tabname)
                    self.logger.info("Table ", tabname, " is in from clause..")
            except Exception as error:
                self.logger.error("Error Occurred in table extraction. Error: " + str(error))
            finally:
                self.connectionHelper.rollback_transaction()
                # self.connectionHelper.execute_sql(
                #    [self.connectionHelper.queries.drop_table(tabname),
                #     self.connectionHelper.queries.alter_table_rename_to("temp", tabname)], self.logger)

    def get_core_relations_by_error(self, query):
        for tabname in self.all_relations:

            try:
                self.connectionHelper.begin_transaction()
                self.connectionHelper.execute_sql(
                    [self.connectionHelper.queries.alter_table_rename_to(tabname, "temp")], self.logger)
                self.app.doJob(query)  # slow
            except Exception as error:
                self.logger.info(str(error))
                if REL_ERROR in str(error):
                    self.core_relations.append(tabname)
                else:
                    self.logger.error("Error Occurred in table extraction. Error: " + str(error))
            finally:
                self.connectionHelper.rollback_transaction()
                # self.connectionHelper.execute_sql(
                #    [self.connectionHelper.queries.alter_table_rename_to("temp", tabname)], self.logger)

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
            self.connectionHelper.execute_sql([self.connectionHelper.set_timeout_to_2s()])
        query, method = self.extract_params_from_args(args)
        if not method:
            method = self.method
        self.core_relations = []
        if method == self.TYPE_RENAME:
            self.get_core_relations_by_rename(query)
        else:
            self.get_core_relations_by_error(query)
        self.connectionHelper.execute_sql([self.connectionHelper.reset_timeout()])
        return self.core_relations

    def get_key_lists(self):
        return self.init.global_key_lists
