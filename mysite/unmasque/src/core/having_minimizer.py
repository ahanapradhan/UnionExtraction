from typing import Any
from mysite.unmasque.src.core.abstract.MinimizerBase import Minimizer
import pandas as pd

class HavingMinimizer(Minimizer):
    def __init__(self, connectionHelper, core_relations, all_sizes, sampling_status):
        super().__init__(connectionHelper, core_relations, all_sizes, "Having Minimizer")
        self.global_min_instance_dict: dict[str, list[Any]] = {}
        self.sampling_status = sampling_status
    
    def extract_params_from_args(self, args):
        # Hidden query `query` as the argument to doJob function
        return super().extract_params_from_args(args)

    def doActualJob(self, args=None):
        query: str = self.extract_params_from_args(args)

        minimized_table_attrib_map: dict[str, list[str]] = dict()

        # Initialize minimized_table_attrib_map
        for reln in self.core_relations:
            minimized_table_attrib_map[reln] = []

        # Copy table to drop FK constraints
        for table in self.core_relations:
            backup_table_name = self.connectionHelper.queries.get_tabname_1(table) if self.sampling_status else self.connectionHelper.queries.get_restore_name(table)
            self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(table, backup_table_name)])
            self.connectionHelper.execute_sql([self.connectionHelper.queries.create_table_as_select_star_from(table, backup_table_name)])

        for table in self.core_relations:
            self.logger.debug(f"Minimizing table {table}")
            is_minimized: bool = False
            while not is_minimized:
                is_minimized = True
                sorted_attrib_val, _ = self._get_frequency_sorted_attrib_value_list(table, minimized_table_attrib_map[table])

                for attrib, value in sorted_attrib_val:
                    if attrib in minimized_table_attrib_map[table]:
                        continue

                    self.connectionHelper.begin_transaction()
                    self._remove_all_rows_except_with_value(table, attrib, value)

                    if self._empty_query_result(query):
                        self.connectionHelper.rollback_transaction()
                        continue

                    is_minimized = False
                    minimized_table_attrib_map[table].append(attrib)
                    self.connectionHelper.commit_transaction()
                    break

        self.logger.debug("First pass done")

        final_pass_flag = False
        while not final_pass_flag:
            final_pass_flag = True
            for table in self.core_relations:
                ctids, _ = self.connectionHelper.execute_sql_fetchall(self.connectionHelper.queries.select_ctid_from(table))
                ctids = [ctid_tup[0] for ctid_tup in ctids]

                for ctid in ctids:
                    self.connectionHelper.begin_transaction()
                    self.connectionHelper.execute_sql([self.connectionHelper.queries.delete_from_where_ctid(table, ctid)])

                    if self._empty_query_result(query):
                        self.connectionHelper.rollback_transaction()
                        continue
                    
                    final_pass_flag = False
                    self.connectionHelper.commit_transaction()

        self.logger.debug("Second pass done")

        if not self.sanity_check(query):
            return False

        self.logger.debug("Having minimizer executed successfully without any errors!")

        self._populate_dict_info()
        return True

    def _get_attributes(self, table_name: str) -> list[str]:
        """
            This function returns a list of all attribute names that make up the
            table `table_name`.
        """
        attrib_infos: list[(str, str, int)] = self.connectionHelper.execute_sql_fetchall(self.connectionHelper.queries.get_column_details_for_table(self.connectionHelper.config.schema, table_name))
        return [str(attrib_name[0]) for attrib_name in attrib_infos[0]]
        
    def _get_frequency_of_values(self, table_name: str, except_in: list[str] = []):
        freq: dict[Any, int] = dict()

        for attrib in self._get_attributes(table_name):
            if attrib in except_in:
                continue

            q = self.connectionHelper.queries.select_attrib_count_from_with_groupby(table_name, attrib)
            val_freqs = self.connectionHelper.execute_sql_fetchall(q)[0]

            for vf in val_freqs:
                v, f = vf
                freq[(attrib, v)] = f

        return freq

    def _get_frequency_sorted_attrib_value_list(self, table_name: str, except_in: list[str] = []):
        freq = self._get_frequency_of_values(table_name, except_in)
        result = list(freq.keys())
        result.sort(key = lambda x: freq[x], reverse = True)

        return result, freq

    def _remove_all_rows_except_with_value(self, table_name: str, attrib_name: str, value: Any) -> None:
        self.connectionHelper.execute_sql([self.connectionHelper.queries.delete_from_where_attrib_not_eq_val(table_name, attrib_name, value)])

    def _empty_query_result(self, query) -> bool:
        res: list[Any] = self.app.doJob(query)
        return len(res) <= 1

    def _populate_dict_info(self):
        # POPULATE MIN INSTANCE DICT
        for tabname in self.core_relations:
            self.global_min_instance_dict[tabname] = []
            sql_query = pd.read_sql_query(self.connectionHelper.queries.get_star(tabname), self.connectionHelper.conn)
            df = pd.DataFrame(sql_query)
            self.global_min_instance_dict[tabname].append(tuple(df.columns))
            for index, row in df.iterrows():
                self.global_min_instance_dict[tabname].append(tuple(row))


