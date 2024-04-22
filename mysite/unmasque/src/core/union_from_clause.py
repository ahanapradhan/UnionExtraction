from ..mocks.database import Schema
from mysite.unmasque.src.core.abstract.AppExtractorBase import AppExtractorBase
from mysite.unmasque.src.core.from_clause import FromClause
from ..util.utils import isQ_result_empty


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
            self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(tab, self.connectionHelper.queries.get_tabname_1(tab)),
                                               self.connectionHelper.queries.create_table_like(tab, self.connectionHelper.queries.get_tabname_1(tab))])

    def run_query(self, QH):
        # self.app_calls += 1
        return self.app.doJob(QH)

    def revert_nullify(self):
        for tab in self.to_nullify:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table(tab),
                                               self.connectionHelper.queries.alter_table_rename_to(self.connectionHelper.queries.get_tabname_1(tab), tab),
                                               self.connectionHelper.queries.drop_table(self.connectionHelper.queries.get_tabname_1(tab))])

    def get_partial_QH(self, QH):
        return self.doJob(QH)

    def isEmpty(self, Res):
        return isQ_result_empty(Res)

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args=None):
        QH = self.extract_params_from_args(args)
        fromTabQ = set(self.get_fromTabs(QH))
        comTabQ = set(self.get_comTabs(QH))
        partTabQ = fromTabQ.difference(comTabQ)
        return partTabQ

    def get_fromTabs(self, QH):
        if self.fromtabs is None:
            self.fromtabs = self.fromClause.doJob(QH, FromClause.TYPE_ERROR)
        self.logger.debug(str(self.fromtabs))
        return self.fromtabs

    def get_comTabs(self, QH):
        if self.comtabs is None:
            self.comtabs = self.fromClause.doJob(QH, FromClause.TYPE_RENAME)
        return self.comtabs

