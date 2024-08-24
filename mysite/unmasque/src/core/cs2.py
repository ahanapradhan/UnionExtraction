import copy

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
    iteration_count = 3
    seed_sample_size_per = 0.16 / sf
    sample_per_multiplier = 10
    sample = {}

    def __init__(self, connectionHelper,
                 all_sizes,
                 core_relations,
                 global_key_lists):
        super().__init__(connectionHelper, "cs2")
        self.passed = False
        self.core_relations = core_relations
        self.global_key_lists = global_key_lists
        self.sizes = all_sizes
        self.enabled = self.connectionHelper.config.use_cs2
        self.all_relations = list(self.sizes.keys())
        self.all_relations.sort()

    def __getSizes_cs(self):
        if not self.sizes:
            for table in self.all_relations:
                self.sizes[table] = self.connectionHelper.execute_sql_with_DictCursor_fetchone_0(
                    self.connectionHelper.queries.get_row_count(table))
        return self.sizes

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args=None):
        sizes = self.__getSizes_cs()

        if not self.connectionHelper.config.use_cs2:
            self.logger.info("Sampling is disabled from config.")
            return False

        query = self.extract_params_from_args(args)

        while self.iteration_count > 0:
            done = self.__correlated_sampling(query, sizes)
            if not done:
                self.logger.info(f"sampling failed on attempt no: {self.iteration_count}")
                self.seed_sample_size_per *= self.sample_per_multiplier
                self.iteration_count -= 1
            else:
                self.passed = True
                self.logger.info("Sampling is successful!")
                return True

        self._restore()
        self.logger.info("Starting with halving based minimization..")
        return False

    def _restore(self):
        for table in self.core_relations:
            backup_tab = self.connectionHelper.queries.get_backup(table)
            self.connectionHelper.execute_sqls_with_DictCursor([
                self.connectionHelper.queries.create_table_like(table,backup_tab),
                self.connectionHelper.queries.insert_into_tab_select_star_fromtab(table, backup_tab)])

    def __correlated_sampling(self, query, sizes):
        self.logger.debug("Starting correlated sampling ")

        # choose base table from each key list> sample it> sample remaining tables based on base table
        for table in self.core_relations:
            self.connectionHelper.execute_sqls_with_DictCursor([self.connectionHelper.queries.create_table_like(table,
                                                                                                                self.connectionHelper.queries.get_backup(
                                                                                                                    table))])

        self.__do_for_key_lists(sizes)

        not_sampled_tables = copy.deepcopy(self.core_relations)
        self.__do_for_empty_key_lists(not_sampled_tables)

        for table in self.core_relations:
            res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(table))
            self.logger.debug(table, res)
            self.sample[table] = res

        # check for null free rows and not just nonempty results
        new_result = self.app.doJob(query)
        if not self.app.isQ_result_nonEmpty_nullfree(new_result):
            for table in self.core_relations:
                self.connectionHelper.execute_sqls_with_DictCursor([self.connectionHelper.queries.drop_table(table)])
                self.sample[table] = sizes[table]
            return False
        return True

    def __do_for_empty_key_lists(self, not_sampled_tables):
        if not len(self.global_key_lists):
            for table in not_sampled_tables:
                self.connectionHelper.execute_sqls_with_DictCursor([f"insert into {table} select * from "
                                                                    f"{self.connectionHelper.queries.get_backup(table)} "
                                                                    f"tablesample system({self.seed_sample_size_per});"])
                res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(table))
                self.logger.debug(table, res)

    def __do_for_key_lists(self, sizes):
        for key_list in self.global_key_lists:
            base_t = get_base_t(key_list, sizes)

            # Sample base table
            base_table = key_list[base_t][0]
            base_key = key_list[base_t][1]
            if base_table in self.core_relations:
                limit_row = sizes[base_table]
                self.connectionHelper.execute_sqls_with_DictCursor([
                    f"insert into {base_table} select * from {self.connectionHelper.queries.get_backup(base_table)} "
                    f"tablesample system({self.seed_sample_size_per}) where ({base_key}) "
                    f"not in (select distinct({base_key}) from {base_table}) Limit {limit_row} ;"])
                res = self.connectionHelper.execute_sql_fetchone_0(
                    self.connectionHelper.queries.get_row_count(base_table))
                self.logger.debug(base_table, res)

            # sample remaining tables from key_list using the sampled base table
            for i in range(0, len(key_list)):
                tabname2 = key_list[i][0]
                key2 = key_list[i][1]

                # if tabname2 in not_sampled_tables:
                if tabname2 != base_table and tabname2 in self.core_relations:
                    limit_row = sizes[tabname2]
                    self.connectionHelper.execute_sqls_with_DictCursor([
                        f"insert into {tabname2} select * from {self.connectionHelper.queries.get_backup(tabname2)} "
                        f"where {key2} in (select distinct({base_key}) from {base_table}) and {key2} "
                        f"not in (select distinct({key2}) from {tabname2}) Limit {limit_row} ;"])
                    res = self.connectionHelper.execute_sql_fetchone_0(
                        self.connectionHelper.queries.get_row_count(tabname2))
                    self.logger.debug(tabname2, res)
