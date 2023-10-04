from .ExtractorBase import Base
from ..executable import Executable
from ..util.common_queries import get_tabname_4


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

    def doJob(self, args):
        super().doJob(args)
        self.restore_d_min()
        self.see_d_min()
        return self.result

    def see_d_min(self):
        for tab in self.core_relations:
            res, des = self.connectionHelper.execute_sql_fetchall("select * from " + f"{tab};")
            self.logger.debug(res)

    def restore_d_min(self):
        for tab in self.core_relations:
            self.connectionHelper.execute_sql(["Truncate table " + f"{tab};",
                                               "Insert into " + f"{tab}" + " Select * from " + f"{get_tabname_4(tab)};"])

    def extract_params_from_args(self, args):
        return args[0]
