import copy
from datetime import date
from typing import Tuple, Union

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase
from .dataclass.genPipeline_context import GenPipelineContext
from .dataclass.pgao_context import PGAOcontext
from ..util.QueryStringGenerator import QueryStringGenerator
from ..util.aoa_utils import get_tab, get_attrib, get_one_tab_attrib_from_aoa_pred


class OuterJoin(GenerationPipeLineBase):

    def __init__(self, connectionHelper, global_pk_dict,
                 genPipelineCtx: GenPipelineContext,
                 q_gen: QueryStringGenerator,
                 genCtx: PGAOcontext):
        super().__init__(connectionHelper, "Outer Join", genPipelineCtx)
        self.global_pk_dict = global_pk_dict
        self.sem_eq_queries = None
        self.importance_dict = {}
        self.projected_attributes = genCtx.projected_attribs
        self.projected_names = genCtx.projection_names
        self.group_by_attrib = genCtx.group_by_attrib
        self.orderby_string = genCtx.orderby_string
        self.Q_E = None
        self.q_gen = q_gen
        self.enabled = self.connectionHelper.config.detect_oj

    def doExtractJob(self, query: str) -> bool:
        self.__resolve_ambigous_projections(query)
        self.Q_E = self.q_gen.formulate_query_string()
        list_of_tables, new_join_graph = self.__get_tables_list_and_new_join_graph()
        if not len(new_join_graph):
            self.logger.info("No Join clause found.")
            return False
        final_edge_seq = self.__create_final_edge_seq(list_of_tables, new_join_graph)
        table_attr_dict = self.__create_table_attrib_dict()
        if table_attr_dict is None:
            self.logger.info("I suppose it is fully equi-join query.")
            return False
        self.__create_importance_dict(new_join_graph, query, table_attr_dict)

        set_possible_queries, fp_on = self.__formulateQueries(final_edge_seq, query)
        self.__remove_semantically_nonEq_queries(new_join_graph, query, set_possible_queries, fp_on)
        self.logger.debug(f"sem eq queries: {self.sem_eq_queries}")
        if not len(self.sem_eq_queries):
            return False
        self.Q_E = self.sem_eq_queries[0]
        return True

    def __resolve_ambigous_projections(self, query):
        replace_dict = dict()
        for attrib in self.projected_attributes:
            idx = self.projected_attributes.index(attrib)
            name = self.projected_names[idx]

            if attrib in self.joined_attribs:
                self.logger.debug("checking for ", attrib)
                table = self.find_tabname_for_given_attrib(attrib)
                prev = self.get_dmin_val(attrib, table)
                mut_val = self.get_other_than_dmin_val_nonText(attrib, table, prev)
                if prev == mut_val:
                    self.logger.info(f"Cannot rectify projection for {attrib}...")
                    continue

                self.update_with_val(attrib, table, mut_val)
                res = self.app.doJob(query)
                self.logger.debug(res)
                self.update_with_val(attrib, table, prev)

                if self.app.isQ_result_has_no_data(res):
                    continue  # cannot do anything as no result is visible

                if not self.app.is_attrib_equal_val(res, name, mut_val):
                    self.identify_projection_rectification(attrib, replace_dict)
        self.projected_attributes, self.group_by_attrib, _, self.orderby_string = self.q_gen.rectify_projection(
            replace_dict)

    def identify_projection_rectification(self, attrib, replace_dict):
        other = attrib
        for edge in self.global_join_graph:
            if attrib in edge:
                idx = edge.index(attrib)
                other = edge[1 - idx]
                break
        replace_dict[attrib] = other
        self.logger.debug(other)

    def __create_final_edge_seq(self, list_of_tables, new_join_graph):
        final_edge_seq = []
        queue = []
        for tab in list_of_tables:
            edge_seq = []
            temp_njg = copy.deepcopy(new_join_graph)
            queue.append(tab)
            while len(queue) != 0:
                remove_edge = []
                table_t = queue.pop(0)
                for edge in temp_njg:
                    if edge[0][1] == table_t and edge[1][1] != table_t:
                        remove_edge.append(edge)
                        edge_seq.append(edge)
                        queue.append(edge[1][1])
                    elif edge[0][1] != table_t and edge[1][1] == table_t:
                        remove_edge.append(list(reversed(edge)))
                        edge_seq.append(list(reversed(edge)))
                        queue.append(edge[0][1])

                for i in remove_edge:
                    if i in temp_njg:
                        temp_njg.remove(i)
                    elif list(reversed(i)) in temp_njg:
                        temp_njg.remove(list(reversed(i)))
                self.logger.debug(temp_njg)
            final_edge_seq.append(edge_seq)
        self.logger.debug("final_edge_seq: ", final_edge_seq)
        return final_edge_seq

    def __create_importance_dict(self, new_join_graph, query, table_attr_dict):
        self.importance_dict = {}
        for edge in new_join_graph:
            key = tuple(edge)
            # modify d1
            # break join condition on this edge
            attrib, table = edge[0][0], edge[0][1]
            s_val = self.get_dmin_val(attrib, table)
            break_val = self.get_different_s_val(attrib, table, s_val)
            self.logger.debug(f"{table}.{attrib} s_val {s_val}, break val {break_val}")
            self.update_with_val(attrib, table, break_val)
            res_hq = self.app.doJob(query)
            self.update_with_val(attrib, table, s_val)

            # make dict of table, attributes: projected val
            loc = {}
            for table in table_attr_dict.keys():
                pj_attrib = table_attr_dict[table]
                if pj_attrib is not None:
                    pj_name = self.projected_names[self.projected_attributes.index(pj_attrib)]
                    loc[pj_attrib] = res_hq[0].index(pj_name)
            self.logger.debug(loc)

            res_hq_dict = {}
            if self.app.isQ_result_has_no_data(res_hq):
                for k in loc.keys():
                    if k not in res_hq_dict.keys():
                        res_hq_dict[k] = [None]
                    else:
                        res_hq_dict[k].append(None)
            else:
                data = res_hq[1:]
                for l in range(len(data)):
                    for k in loc.keys():
                        if k not in res_hq_dict.keys():
                            res_hq_dict[k] = [data[l][loc[k]]]
                        else:
                            res_hq_dict[k].append(data[l][loc[k]])
            self.logger.debug(res_hq_dict)

            self.importance_dict[key] = {}
            p_att_table1 = self.__make_importance_dict_entry(key, edge[0][1], res_hq_dict, table_attr_dict)
            p_att_table2 = self.__make_importance_dict_entry(key, edge[1][1], res_hq_dict, table_attr_dict)
            self.logger.debug(p_att_table1, p_att_table2)

        self.logger.debug(self.importance_dict)

    def __make_importance_dict_entry(self, key, table, res_hq_dict, table_attr_dict):
        p_att_table = None
        attrib = table_attr_dict[table]
        if attrib in res_hq_dict.keys():
            for i in res_hq_dict[attrib]:
                if i not in [None, 'None']:
                    p_att_table = 10
                    break
        priority = 'h' if p_att_table is not None else 'l'
        self.importance_dict[key][table] = priority
        return p_att_table

    def __create_table_attrib_dict(self):
        self.logger.debug(f"Projected attribs: {self.projected_attributes}")
        # once dict is made compare values to null or not null
        # and prepare importance_dict
        table_attr_dict = {}
        # for each table find a projected attribute which we will check for null values
        for k in self.projected_attributes:
            if k is None or k == '':
                continue
            tabname = self.find_tabname_for_given_attrib(k)
            self.logger.debug("attrib: ", k, "table: ", tabname)
            if tabname not in table_attr_dict.keys():
                self.logger.debug(k, tabname)
                table_attr_dict[tabname] = k
        for tab in self.core_relations:
            if tab not in table_attr_dict.keys():
                table_attr_dict[tab] = None
                self.logger.error(f"ERROR: {tab} does not have any direct projection! Cannot verify outer-join! Bye.")
                return None
        self.logger.debug("table_attr_dict: ", table_attr_dict)
        return table_attr_dict

    def __get_tables_list_and_new_join_graph(self):
        new_join_graph = []
        list_of_tables = []
        for edge in self.global_join_graph:
            temp = []
            if len(edge) > 2:
                i = 0
                while i < (len(edge)) - 1:
                    temp = []
                    self.__add_tabname_for_attrib(edge[i], list_of_tables, temp)
                    self.__add_tabname_for_attrib(edge[i + 1], list_of_tables, temp)

                    i = i + 1
                    new_join_graph.append(temp)
            else:
                for vertex in edge:
                    self.__add_tabname_for_attrib(vertex, list_of_tables, temp)
                new_join_graph.append(temp)
        self.logger.debug("list_of_tables: ", list_of_tables)
        self.logger.debug("new_join_graph: ", new_join_graph)
        return list_of_tables, new_join_graph

    def __remove_semantically_nonEq_queries(self, new_join_graph, query, set_possible_queries, on_predicates):
        # eliminate semanticamy non-equivalent querie from set_possible_queries
        # this code needs to be finished (27 feb)
        sem_eq_queries = []

        for num in range(0, len(set_possible_queries)):
            poss_q = set_possible_queries[num]
            same = True
            for edge in new_join_graph:
                attrib, table = edge[0][0], edge[0][1]
                s_val = self.get_dmin_val(attrib, table)
                break_val = self.get_different_s_val(attrib, table, s_val)
                self.logger.debug(f"{table}.{attrib} s_val {s_val}, break val {break_val}")
                self.update_with_val(attrib, table, break_val)
                same = self.__are_the_results_same(poss_q, query, same)
                self.update_with_val(attrib, table, s_val)
            for fp in on_predicates:
                attrib, tab = fp[1], fp[0]
                self.logger.debug(fp, attrib, tab)
                prev = self.get_dmin_val(attrib, tab)
                self.logger.debug(f"{tab}.{attrib} s_val {prev}, break val NULL")
                self.update_with_val(attrib, tab, 'NULL')
                same = self.__are_the_results_same(poss_q, query, same)
                self.update_with_val(attrib, tab, prev)

            if same:
                sem_eq_queries.append(poss_q)

        self.sem_eq_queries = sem_eq_queries

    def __are_the_results_same(self, poss_q, query, same):
        # result of hidden query
        res_HQ = self.app.doJob(query)
        # result of extracted query
        res_poss_q = self.app.doJob(poss_q)
        #  maybe needs  work
        if len(res_HQ) != len(res_poss_q):
            same = False
        else:
            data_HQ = res_HQ[1:]
            data_poss_q = res_poss_q[1:]
            # maybe use the available result comparator techniques
            for var in range(len(data_HQ)):
                self.logger.debug(data_HQ[var] == data_poss_q[var])
                if not (data_HQ[var] == data_poss_q[var]):
                    self.logger.debug(data_HQ[var])
                    self.logger.debug(data_poss_q[var])
                    same = False
        return same

    def __add_tabname_for_attrib(self, attrib, list_of_tables, temp):
        tabname = self.find_tabname_for_given_attrib(attrib)
        if tabname not in list_of_tables:
            list_of_tables.append(tabname)
        self.logger.debug(attrib, tabname)
        temp.append((attrib, tabname))

    def update_attrib_to_see_impact(self, attrib: str, tabname: str):
        prev = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.select_attribs_from_relation([attrib], tabname))
        val = 'NULL'
        self.logger.debug(f"update {tabname}.{attrib} with value {val} that had previous value {prev}")
        self.update_with_val(attrib, tabname, val)
        return val, prev

    def __formulateQueries(self, final_edge_seq, query):
        fp_on, fp_where = self.__determine_on_and_where_filters(query)
        aoa_on, aoa_where = self.__determine_on_and_where_aoa(query)
        set_possible_queries = []
        for seq in final_edge_seq:
            self.q_gen.backup_query_before_new_generation()
            self.q_gen.clear_from_where_ops()
            for edge in seq:
                table1, table2 = edge[0][1], edge[1][1]
                imp_t1, imp_t2 = self.__determine_join_edge_type(edge, table1, table2)
                self.q_gen.generate_from_on_clause(edge, fp_on + aoa_on, imp_t1, imp_t2, table1, table2)
            self.q_gen.generate_where_clause(fp_where + aoa_where)
            self.q_gen.generate_groupby_select()
            q_candidate = self.q_gen.write_query()
            self.logger.debug("+++++++++++++++++++++")
            if q_candidate.count('OUTER'):
                set_possible_queries.append(q_candidate)

        for q in set_possible_queries:
            self.logger.debug(q)

        return set_possible_queries, fp_on

    def __determine_on_and_where_aoa(self, query):
        aoa_pred_on, aoa_pred_where = [], []
        all_aoa = self.q_gen.algebraic_inequalities
        self.logger.debug("all_aoa predicates: ", all_aoa)
        for aoa in all_aoa:
            one, op, other = aoa[0], aoa[1], aoa[2]
            prev_one = self.connectionHelper.execute_sql_fetchone_0(
                self.connectionHelper.queries.select_attribs_from_relation([get_attrib(one)], get_tab(one)))
            prev_other = self.connectionHelper.execute_sql_fetchone_0(
                self.connectionHelper.queries.select_attribs_from_relation([get_attrib(other)], get_tab(other)))

            if op == '<':
                self.update_with_val(get_attrib(one), get_tab(one), prev_other)
                self.update_with_val(get_attrib(other), get_tab(other), prev_one)

            else:  # '<='
                new_one = self.get_other_than_dmin_val_nonText(get_attrib(one), get_tab(one), prev_one)
                new_other = self.get_other_than_dmin_val_nonText(get_attrib(other), get_tab(other), prev_other)
                if new_one < prev_one:
                    self.update_with_val(get_attrib(other), get_tab(other), new_one)
                elif new_other > prev_other:
                    self.update_with_val(get_attrib(one), get_tab(one), new_other)

            res_hq = self.app.doJob(query)
            self.logger.debug(f"res_hq: {res_hq}")
            if len(res_hq) == 1:
                aoa_pred_where.append(aoa)
            else:
                aoa_pred_on.append(aoa)
            self.update_with_val(get_attrib(one), get_tab(one), prev_one)
            self.update_with_val(get_attrib(other), get_tab(other), prev_other)

        self.logger.debug(aoa_pred_on, aoa_pred_where)
        return aoa_pred_on, aoa_pred_where

    def __determine_on_and_where_filters(self, query):
        filter_pred_on, filter_pred_where = [], []
        all_arithmetic_filters = self.q_gen.all_arithmetic_filters
        self.logger.debug("all_arithmetic_filters: ", all_arithmetic_filters)
        for fp in all_arithmetic_filters:
            self.logger.debug(f"fp from global filter predicates: {fp}")
            tab, attrib = fp[0], fp[1]
            self.__check_on_or_where(tab, attrib, filter_pred_on, filter_pred_where, fp, query)
        self.logger.debug(filter_pred_on, filter_pred_where)
        return filter_pred_on, filter_pred_where

    def __check_on_or_where(self, tab, attrib, filter_pred_on, filter_pred_where, fp, query):
        _, prev = self.update_attrib_to_see_impact(attrib, tab)
        res_hq = self.app.doJob(query)
        self.logger.debug(f"res_hq: {res_hq}")
        if len(res_hq) == 1:
            filter_pred_where.append(fp)
        else:
            filter_pred_on.append(fp)
        self.update_with_val(attrib, tab, prev)

    def __determine_join_edge_type(self, edge, table1, table2):
        # steps to determine type of join for edge
        if tuple(edge) in self.importance_dict.keys():
            imp_t1 = self.importance_dict[tuple(edge)][table1]
            imp_t2 = self.importance_dict[tuple(edge)][table2]
        elif tuple(list(reversed(edge))) in self.importance_dict.keys():
            imp_t1 = self.importance_dict[tuple(list(reversed(edge)))][table1]
            imp_t2 = self.importance_dict[tuple(list(reversed(edge)))][table2]
        else:
            self.logger.debug("error sneha!!!")
        self.logger.debug(imp_t1, imp_t2)
        return imp_t1, imp_t2
