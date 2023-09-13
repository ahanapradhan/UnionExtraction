from ...refactored.abstract.ExtractorBase import Base
from ...refactored.util.common_queries import alter_table_rename_to, create_table_like, get_tabname_un, \
    drop_table
from ..core import algorithm1, OldPipeLine
from ..core.UN1_from_clause import UN1FromClause


class Minus(Base):
    working_dir = "/Users/ahanapradhan/Desktop/Projects/UNMASQUE/reduced_data/"

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Minus")
        self.core_relations_q1 = None
        self.core_relations = None
        self.core_relations_q2 = None
        self.key_lists = None
        self.all_relations = None
        self.global_pk_dict = None
        self.db = UN1FromClause(self.connectionHelper)

    def extract_params_from_args(self, args):
        return args[0]

    def init(self, query):
        self.connectionHelper.connectUsingParams()
        self.core_relations_q2, self.core_relations = algorithm1.algo(self.db, query)
        self.core_relations_q1 = self.db.comtabs
        self.all_relations = self.db.get_relations()
        self.key_lists = self.db.fromClause.init.global_key_lists
        self.global_pk_dict = self.db.fromClause.init.global_pk_dict
        self.connectionHelper.closeConnection()

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        self.init(query)
        q1 = self.extract_a_query(self.core_relations_q1, self.core_relations_q2, query)
        cap_q = "(" + q1[:-1] + ") EXCEPT (" + query[:-1] + ")"
        q2 = self.extract_a_query(list(self.core_relations_q2), set(), cap_q)
        return q1, q2

    def extract_a_query(self, core_relations_q1, core_relations_q2, query):
        self.connectionHelper.connectUsingParams()
        self.nullify_relations(core_relations_q2)
        eq, _ = OldPipeLine.extract(self.connectionHelper,
                                    query,
                                    self.all_relations,
                                    core_relations_q1,
                                    self.key_lists,
                                    self.global_pk_dict)
        self.revert_nullifications(core_relations_q2)
        self.connectionHelper.closeConnection()
        return eq

    def nullify_relations(self, relations):
        for tab in relations:
            self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_un(tab)),
                                               create_table_like(tab, get_tabname_un(tab))])

    def revert_nullifications(self, relations):
        for tab in relations:
            self.connectionHelper.execute_sql([drop_table(tab),
                                               alter_table_rename_to(get_tabname_un(tab), tab),
                                               drop_table(get_tabname_un(tab))])

    def export_vm_data(self, tab):
        self.connectionHelper.execute_sql(["COPY " + tab + " TO '"
                                           + self.working_dir + tab
                                           + "_q1.csv' DELIMITER ',' CSV HEADER;"])
