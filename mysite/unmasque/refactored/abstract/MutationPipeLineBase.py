from .ExtractorBase import Base
from ..executable import Executable
from ..util.common_queries import get_tabname_4, get_star, truncate_table, create_table_as_select_star_from, \
    insert_into_tab_select_star_fromtab


class MutationPipeLineBase(Base):

    def __init__(self, connectionHelper,
                 core_relations,
                 global_min_instance_dict,
                 name):
        super().__init__(connectionHelper, name)
        self.app = Executable(connectionHelper)
        # from from clause
        self.core_relations = core_relations
        # from view minimizer
        self.global_min_instance_dict = global_min_instance_dict
        self.mock = False

    def doJob(self, args):
        super().doJob(args)
        if not self.mock:
            self.restore_d_min()
            # self.see_d_min()
        return self.result

    def see_d_min(self):
        pass
        '''
        for tab in self.core_relations:
            res, des = self.connectionHelper.execute_sql_fetchall(get_star(tab))
            self.logger.debug(res)
        '''

    def restore_d_min(self):
        for tab in self.core_relations:
            self.connectionHelper.execute_sql([truncate_table(tab),
                                               insert_into_tab_select_star_fromtab(tab, get_tabname_4(tab))])

    def truncate_core_relations(self):
        # Truncate all core relations
        for table in self.core_relations:
            self.connectionHelper.execute_sql([truncate_table(table)])

    def extract_params_from_args(self, args):
        return args[0]
