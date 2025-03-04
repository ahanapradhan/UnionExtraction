from .factory.error_handling import UnmasqueError
from ..util.errorcodes import ERROR_006
from ...src.core.abstract.AppExtractorBase import AppExtractorBase
from ...src.core.initialization import Initiator
from ...src.util.application_type import ApplicationType
from ...src.util.constants import REL_ERROR


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

    def get_core_relations_by_void(self, query):
        for tabname in self.all_relations:
            try:
                self.connectionHelper.begin_transaction()
                self.connectionHelper.execute_sql(
                    [self.connectionHelper.queries.alter_table_rename_to(self.get_original_table_name(tabname),
                                                                         self._get_dirty_name(tabname)),
                     self.connectionHelper.queries.create_table_like(
                         self.get_original_table_name(tabname),
                         self.get_original_table_name(self._get_dirty_name(tabname)))], self.logger)
                new_result = self.app.doJob(query)
                if self.app.isQ_result_no_full_nullfree_row(new_result):
                    self.core_relations.append(tabname)
                    self.logger.info("Table ", tabname, " is in from clause..")
            except Exception as error:
                self.logger.error("Error Occurred in table extraction. Error: " + str(error))
            finally:
                self.connectionHelper.rollback_transaction()

    def get_core_relations_by_error(self, query):
        for tabname in self.all_relations:
            try:
                self.connectionHelper.begin_transaction()
                self.connectionHelper.execute_sql(
                    [self.connectionHelper.queries.alter_table_rename_to(self.get_original_table_name(tabname),
                                                                         self._get_dirty_name(tabname))], self.logger)

                if self.timeout:
                    self.connectionHelper.execute_sql([self.connectionHelper.set_timeout_to_2s()])
                result = self.app.doJob(query)  # slow
                if REL_ERROR in result:
                    self.core_relations.append(tabname)
                self.connectionHelper.execute_sql([self.connectionHelper.reset_timeout()])
            except Exception as error:
                self.logger.info(str(error))
            finally:
                self.connectionHelper.rollback_transaction()

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

    def doActualJob(self, args=None):
        setup_done = self.setup()
        if not setup_done:
            return False

        query, method = self.extract_params_from_args(args)
        if not method:
            method = self.method
        self.core_relations = []
        if method == self.TYPE_RENAME:
            self.get_core_relations_by_void(query)
        else:
            self.get_core_relations_by_error(query)
        if len(self.core_relations) == 0:
            raise UnmasqueError(ERROR_006, "from_clause", {})
        return self.core_relations

    def get_key_lists(self):
        return self.init.global_key_lists
