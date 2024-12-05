import copy
import math

from ...src.core.abstract.AppExtractorBase import AppExtractorBase


def get_base_t(key_list, sizes):
    max_cs = 0
    base_t = 0
    for i in range(0, len(key_list)):
        if max_cs < sizes[key_list[i][0]]:
            max_cs = sizes[key_list[i][0]]
            base_t = i
    return base_t


class Cs2(AppExtractorBase):
    sf = 1
    seed_sample_size_per = 0.16 / sf
    sample_per_multiplier = 2
    sample = {}
    TOTAL_perc = 100
    MAX_iter = 3

    def __init__(self, connectionHelper,
                 all_sizes,
                 core_relations,
                 global_key_lists, perc_based_cutoff=False, name="cs2"):
        super().__init__(connectionHelper, name)
        self.passed = False
        self.iteration_count = 0
        self.core_relations = core_relations
        self.global_key_lists = global_key_lists
        self.sizes = all_sizes
        self.enabled = self.connectionHelper.config.use_cs2
        self.all_relations = list(self.sizes.keys())
        self.all_relations.sort()
        self.perc_based_cutoff = perc_based_cutoff

    def _dont_stop_trying(self):
        if self.perc_based_cutoff:
            return self.seed_sample_size_per < self.TOTAL_perc
        else:
            return self.iteration_count < self.MAX_iter

    def __getSizes_cs(self):
        # if not self.sizes:
        for table in self.all_relations:
            self.sizes[table] = self.connectionHelper.execute_sql_with_DictCursor_fetchone_0(
                self.connectionHelper.queries.get_row_count(self.get_fully_qualified_table_name(table)),
                self.logger)
        return self.sizes

    def extract_params_from_args(self, args):
        self.iteration_count = 0
        return args[0]

    def _truncate_tables(self):
        for table in self.core_relations:
            self.connectionHelper.execute_sql(
                [self.connectionHelper.queries.truncate_table(self.get_fully_qualified_table_name(table))], self.logger)

    def doActualJob(self, args=None):
        query = self.extract_params_from_args(args)
        to_truncate = True
        while self._dont_stop_trying():
            done = self.__correlated_sampling(query, self.sizes, to_truncate)
            to_truncate = False  # first time truncation is sufficient, each for each union flow
            if not done:
                self.logger.info(f"sampling failed on attempt no: {self.iteration_count}")
                self.seed_sample_size_per *= self.sample_per_multiplier
                self.iteration_count = self.iteration_count + 1
            else:
                self.passed = True
                self.logger.info("Sampling is successful!")
                self.logger.info(f"Sampling Percentage: {self.seed_sample_size_per}")
                sizes = self.__getSizes_cs()
                self.logger.info(sizes)
                self.connectionHelper.commit_transaction()
                return True

        self._restore()
        return False

    def _restore(self):
        for table in self.core_relations:
            backup_tab = self.get_original_table_name(table)
            self.connectionHelper.execute_sqls_with_DictCursor([
                self.connectionHelper.queries.create_table_like(
                    self.get_fully_qualified_table_name(table), self.get_original_table_name(table)),
                self.connectionHelper.queries.insert_into_tab_select_star_fromtab(
                    self.get_fully_qualified_table_name(table), self.get_original_table_name(table))], self.logger)

    def __correlated_sampling(self, query, sizes, to_truncate=False):
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

        # check for null free rows and not just nonempty results
        new_result = self.app.doJob(query)
        # self.logger.debug(f"result after sampling: {new_result}")
        if not self.app.isQ_result_nonEmpty_nullfree(new_result):
            for table in self.core_relations:
                self.connectionHelper.execute_sqls_with_DictCursor([self.connectionHelper.queries.drop_table(
                    self.get_fully_qualified_table_name(table))], self.logger)
                self.sample[table] = sizes[table]
            return False
        return True

    def __do_for_empty_key_lists(self, not_sampled_tables):
        if not len(self.global_key_lists):
            for table in not_sampled_tables:
                res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(
                    self.get_fully_qualified_table_name(table)))
                self.logger.debug("before sample insertion: ", table, res)
                self.connectionHelper.execute_sqls_with_DictCursor(
                    [
                        f"insert into {self.get_fully_qualified_table_name(table)} (select * from "f"{self.get_original_table_name(table)} "
                        f"tablesample system({self.seed_sample_size_per}));"])
                res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(
                    self.get_fully_qualified_table_name(table)))
                self.logger.debug(table, res)

    def __do_for_key_lists(self, sizes):
        for key_list in self.global_key_lists:
            base_t = get_base_t(key_list, sizes)

            # Sample base table
            base_table, base_key = key_list[base_t][0], key_list[base_t][1]
            if base_table in self.core_relations:
                # limit_row = int(math.ceil(min(sizes[base_table], sizes[base_table] * self.seed_sample_size_per)))
                limit_row = sizes[base_table]
                self.connectionHelper.execute_sqls_with_DictCursor([
                    f"insert into {self.get_fully_qualified_table_name(base_table)} "
                    f"(select * from {self.get_original_table_name(base_table)} "
                    f"tablesample system({self.seed_sample_size_per}) where ({base_key}) "
                    f"not in (select distinct({base_key}) from {self.get_fully_qualified_table_name(base_table)}) Limit {limit_row} );"],
                    self.logger)

                res = self.connectionHelper.execute_sql_fetchone_0(
                    self.connectionHelper.queries.get_row_count(self.get_fully_qualified_table_name(base_table)))
                self.logger.debug(base_table, res)

            # sample remaining tables from key_list using the sampled base table
            for key_item in key_list:
                sampled_table, key = key_item[0], key_item[1]

                # if sampled_table in not_sampled_tables:
                if sampled_table != base_table and sampled_table in self.core_relations:
                    limit_row = sizes[sampled_table]
                    self.connectionHelper.execute_sqls_with_DictCursor([
                        f"insert into {self.get_fully_qualified_table_name(sampled_table)} (select * from "
                        f"{self.get_original_table_name(sampled_table)} "
                        f"where {key} in (select distinct({base_key}) from {self.get_fully_qualified_table_name(base_table)}) and {key} "
                        f"not in (select distinct({key}) from {self.get_fully_qualified_table_name(sampled_table)}) Limit {limit_row}) ;"],
                        self.logger)
                    res = self.connectionHelper.execute_sql_fetchone_0(
                        self.connectionHelper.queries.get_row_count(self.get_fully_qualified_table_name(sampled_table)))
                    self.logger.debug(sampled_table, res)
