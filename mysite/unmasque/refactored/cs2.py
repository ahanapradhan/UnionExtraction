import copy

from .abstract.AppExtractorBase import AppExtractorBase
from ..refactored.util.common_queries import get_row_count, drop_table, alter_table_rename_to, \
    get_restore_name, create_table_like
from ..refactored.util.utils import isQ_result_empty


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
                 all_relations,
                 core_relations,
                 global_key_lists):
        super().__init__(connectionHelper, "cs2")
        self.passed = False
        self.all_relations = all_relations
        self.core_relations = core_relations
        self.global_key_lists = global_key_lists
        self.sizes = {}

    def getSizes_cs(self):
        if not self.sizes:
            for table in self.all_relations:
                self.sizes[table] = self.connectionHelper.execute_sql_with_DictCursor_fetchone_0(get_row_count(table))
        return self.sizes

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        sizes = self.getSizes_cs()

        if not self.connectionHelper.config.use_cs2:
            self.logger.info("Sampling is disabled from config.")
            return False

        self.take_backup()
        query = self.extract_params_from_args(args)

        while self.iteration_count > 0:
            done = self.correlated_sampling(query, sizes)
            if not done:
                self.logger.info(f"sampling failed on attempt no: {self.iteration_count}")
                self.seed_sample_size_per *= self.sample_per_multiplier
                self.iteration_count -= 1
            else:
                self.passed = True
                self.logger.info("Sampling is successful!")
                return True

        self.restore()
        self.logger.info("Starting with halving based minimization..")
        return False

    def take_backup(self):
        for table in self.core_relations:
            self.connectionHelper.execute_sqls_with_DictCursor([drop_table(get_restore_name(table)),
                                                                alter_table_rename_to(table, get_restore_name(table))])

    def restore(self):
        for table in self.core_relations:
            self.connectionHelper.execute_sqls_with_DictCursor([alter_table_rename_to(get_restore_name(table), table)])

    def correlated_sampling(self, query, sizes):
        self.logger.debug("Starting correlated sampling ")

        # choose base table from each key list> sample it> sample remaining tables based on base table
        for table in self.core_relations:
            self.connectionHelper.execute_sqls_with_DictCursor([create_table_like(table, get_restore_name(table))])

        self.do_for_key_lists(sizes)

        not_sampled_tables = copy.deepcopy(self.core_relations)
        self.do_for_empty_key_lists(not_sampled_tables)

        for table in self.core_relations:
            res = self.connectionHelper.execute_sql_fetchone_0(get_row_count(table))
            self.logger.debug(table, res)
            self.sample[table] = res

        # check for null free rows and not just nonempty results
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            for table in self.core_relations:
                self.connectionHelper.execute_sqls_with_DictCursor([drop_table(table)])
                self.sample[table] = sizes[table]
            return False
        return True

    def do_for_empty_key_lists(self, not_sampled_tables):
        if len(self.global_key_lists) == 0:
            for table in not_sampled_tables:
                self.connectionHelper.execute_sqls_with_DictCursor(["insert into " + table +
                                                                    " select * from " + get_restore_name(table)
                                                                    + " tablesample system("
                                                                    + str(self.seed_sample_size_per) + ");"])
                res = self.connectionHelper.execute_sql_fetchone_0(get_row_count(table))
                self.logger.debug(table, res)

    def do_for_key_lists(self, sizes):
        for key_list in self.global_key_lists:
            base_t = get_base_t(key_list, sizes)

            # Sample base table
            base_table = key_list[base_t][0]
            base_key = key_list[base_t][1]
            if base_table in self.core_relations:
                limit_row = sizes[base_table]
                self.connectionHelper.execute_sqls_with_DictCursor([
                    "insert into " + base_table
                    + " select * from " + get_restore_name(base_table)
                    + " tablesample system(" + str(self.seed_sample_size_per) + ") where ("
                    + base_key + ") not in (select distinct("
                    + base_key + ") from "
                    + base_table + ")  Limit " + str(limit_row) + " ;"])
                res = self.connectionHelper.execute_sql_fetchone_0(get_row_count(base_table))
                self.logger.debug(base_table, res)

            # sample remaining tables from key_list using the sampled base table
            for i in range(0, len(key_list)):
                tabname2 = key_list[i][0]
                key2 = key_list[i][1]

                # if tabname2 in not_sampled_tables:
                if tabname2 != base_table and tabname2 in self.core_relations:
                    limit_row = sizes[tabname2]
                    self.connectionHelper.execute_sqls_with_DictCursor([
                        f"insert into {tabname2} select * from {tabname2}_restore "
                        f"where {key2} in (select distinct({base_key}) from {base_table}) "
                        f"and {key2} not in (select distinct({key2}) from {tabname2}) Limit {str(limit_row)} ;"])
                    res = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname2))
                    self.logger.debug(tabname2, res)
