from ...src.core.cs2 import Cs2
from ...src.util.constants import SCALE_DOWN, WORKING_SCHEMA


class ScaleDown(Cs2):
    def __init__(self, connectionHelper,
                 all_sizes,
                 core_relations,
                 global_key_lists):
        super().__init__(connectionHelper, all_sizes, core_relations, global_key_lists, True, "Scale Down", connectionHelper.config.sf)
        self.downscale_schema = f"{WORKING_SCHEMA}{SCALE_DOWN}"
        self.full_schema = self.connectionHelper.config.user_schema
        self.enabled = self.connectionHelper.config.scale_down

    def extract_params_from_args(self, args):
        return args[0]

    def __create_schema(self):
        self.connectionHelper.execute_sql([f"Create Schema {self.downscale_schema};"], self.logger)

    def __delete_schema(self):
        self.connectionHelper.execute_sql([f"Drop Schema if exists {self.downscale_schema} cascade;"],
                                          self.logger)

    def get_fully_qualified_table_name(self, table):
        return f"{self.downscale_schema}.{table}"

    def _restore(self):
        # self.__delete_schema()
        pass

    def doActualJob(self, args=None):
        self.__delete_schema()
        self.__create_schema()
        self.set_data_schema(self.downscale_schema)
        for table in self.core_relations:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.create_table_like(
                self.get_fully_qualified_table_name(table), self.get_original_table_name(table))], self.logger)
        check = super().doActualJob(self.extract_params_from_args(args))
        if not check:
            self.set_data_schema()
        else:
            self.logger.info("Hopefully Scaling Down Worked!")
            self.connectionHelper.config.user_schema = self.downscale_schema
            print(self.seed_sample_size_per)
        return check
