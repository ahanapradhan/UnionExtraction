from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.from_clause import FromClause
from mysite.unmasque.refactored.util.common_queries import alter_table_rename_to, create_table_like
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.mocks.database import Schema


class UN1FromClause(Schema, Base):
    relations = []

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Old Unmasque")
        self.fromClause = FromClause(connectionHelper)

    def get_relations(self):
        self.fromClause.init_check()
        return self.fromClause.all_relations

    def nullify_except(self, s_set):
        to_nullify = set(self.get_relations()).difference(s_set)
        for tab in to_nullify:
            self.connectionHelper.execute_sql([alter_table_rename_to(tab, str(tab + "1")),
                                               create_table_like(tab, str(tab + "1"))])

    def run_query(self, QH):
        return self.fromClause.app.doJob(QH)

    def revert_nullify(self):
        self.connectionHelper.execute_sql(["ROLLBACK;"])

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
        return self.fromClause.doJob([QH, "error"])

    def get_comTabs(self, QH):
        return self.fromClause.doJob([QH, "rename"])
