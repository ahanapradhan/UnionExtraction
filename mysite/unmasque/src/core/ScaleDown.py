import copy

from ...src.core.cs2 import Cs2
from ...src.util.constants import SCALE_DOWN, WORKING_SCHEMA


class ScaleDown(Cs2):
    def __init__(self, connectionHelper,
                 all_sizes,
                 core_relations,
                 global_key_lists):
        super().__init__(connectionHelper, all_sizes, core_relations, global_key_lists, True, "Scale Down",
                         connectionHelper.config.sf)
        self.downscale_schema = f"{WORKING_SCHEMA}{SCALE_DOWN}"
        self.full_schema = self.connectionHelper.config.user_schema
        self.enabled = self.connectionHelper.config.scale_down
        self.seed_sample_size_per = self.seed_sample_size_per * pow(self.sample_per_multiplier,
                                                                    connectionHelper.config.scale_retry)
        print(f"seed_sample_size_per {self.seed_sample_size_per}")

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

    def doAppCountJob(self, args):  # no need to app count for scaling down
        self.__delete_schema()
        self.__create_schema()
        self.set_data_schema(self.downscale_schema)
        for table in self.core_relations:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.create_table_like(
                self.get_fully_qualified_table_name(table), self.get_original_table_name(table))], self.logger)
        check = self.doActualJob(self.extract_params_from_args(args))
        if not check:
            self.set_data_schema()
        else:
            self.logger.info("Hopefully Scaling Down Worked!")
            print("Hopefully Scaling Down Worked!")
            self.connectionHelper.config.user_schema = self.downscale_schema
            print(self.seed_sample_size_per)
            print(self.sample)
        return check

    def _correlated_sampling(self, query, sizes, to_truncate=False):
        self.logger.debug("Starting correlated sampling ")

        # choose base table from each key list> sample it> sample remaining tables based on base table
        for table in self.all_relations:
            self.connectionHelper.execute_sqls_with_DictCursor(
                [self.connectionHelper.queries.create_table_like(self.get_fully_qualified_table_name(table),
                                                                 self.get_original_table_name(table))], self.logger)
        if to_truncate:
            self._truncate_tables()
        self.__do_for_key_lists(sizes)

        not_sampled_tables = copy.deepcopy(self.core_relations)
        self.__do_for_empty_key_lists(not_sampled_tables)

        for table in self.core_relations:
            res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(
                self.get_fully_qualified_table_name(table)), self.logger)
            self.logger.debug(table, res)
            self.sample[table] = res

        for q in query:
            # check for null free rows and not just nonempty results
            new_result = self.app.doJob(q)
            # self.logger.debug(f"result after sampling: {new_result}")
            if not self.app.isQ_result_nonEmpty_nullfree(new_result):
                for table in self.core_relations:
                    self.connectionHelper.execute_sqls_with_DictCursor([self.connectionHelper.queries.drop_table(
                        self.get_fully_qualified_table_name(table))], self.logger)
                    self.sample[table] = sizes[table]
                return False
            self.logger.debug(f"{q} is not satisfied!")
        return True
