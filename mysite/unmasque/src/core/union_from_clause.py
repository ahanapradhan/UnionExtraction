from ..mocks.database import Schema
from ...refactored.abstract.AppExtractorBase import AppExtractorBase
from ...refactored.from_clause import FromClause
from ...refactored.util.common_queries import alter_table_rename_to, create_table_like, drop_table, \
    get_tabname_1
from ...refactored.util.utils import isQ_result_empty


class UnionFromClause(Schema, AppExtractorBase):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Union From Clause")
        self.comtabs = None
        self.fromtabs = None
        self.to_nullify = None
        self.fromClause = FromClause(connectionHelper)
        self.relations = []

    def get_relations(self):
        if not self.fromClause.init.done:
            self.fromClause.init.doJob()
        return self.fromClause.all_relations

    def nullify_except(self, s_set):
        self.to_nullify = s_set.difference(self.comtabs)
        self.logger.debug("to nullify " + str(self.to_nullify))
        for tab in self.to_nullify:
            self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_1(tab)),
                                               create_table_like(tab, get_tabname_1(tab))])

    def run_query(self, QH):
        self.app_calls += 1
        return self.app.doJob(QH)

    def revert_nullify(self):
        for tab in self.to_nullify:
            self.connectionHelper.execute_sql([drop_table(tab),
                                               alter_table_rename_to(get_tabname_1(tab), tab),
                                               drop_table(get_tabname_1(tab))])

    def get_partial_QH(self, QH):
        return self.doJob(QH)

    def isEmpty(self, Res):
        return isQ_result_empty(Res)

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        QH = self.extract_params_from_args(args)
        fromTabQ = set(self.get_fromTabs(QH))
        comTabQ = set(self.get_comTabs(QH))
        partTabQ = fromTabQ.difference(comTabQ)
        return partTabQ

    def get_fromTabs(self, QH):
        if self.fromtabs is None:
            self.fromtabs = self.fromClause.doJob(QH, "error")
        self.logger.debug(str(self.fromtabs))
        return self.fromtabs

    def get_comTabs(self, QH):
        if self.comtabs is None:
            self.comtabs = self.fromClause.doJob(QH, "rename")
        return self.comtabs

