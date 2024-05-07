import copy
from datetime import date
from typing import Tuple, Union

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase


class OuterJoin(GenerationPipeLineBase):
    join_map = {('l', 'l'): ' INNER JOIN ', ('l', 'h'): ' RIGHT OUTER JOIN ',
                ('h', 'l'): ' LEFT OUTER JOIN ', ('h', 'h'): ' FULL OUTER JOIN '}

    def __init__(self, connectionHelper, global_pk_dict, delivery, projected_attributes, q_gen, projected_names):
        super().__init__(connectionHelper, "Outer Join", delivery)
        self.global_pk_dict = global_pk_dict
        self.check_nep_again = False
        self.sem_eq_queries = None
        self.sem_eq_listdict = {}
        self.importance_dict = {}
        self.projected_attributes = projected_attributes
        self.projected_names = projected_names
        self.Q_E = None
        self.q_gen = q_gen
        self.enabled = self.connectionHelper.config.detect_oj

    def doExtractJob(self, query: str) -> bool:
        list_of_tables, new_join_graph = self.get_tables_list_and_new_join_graph()
        if not len(new_join_graph):
            self.logger.info("No Join clause found.")
            return True
        final_edge_seq = self.create_final_edge_seq(list_of_tables, new_join_graph)
        table_attr_dict = self.create_table_attrib_dict()
        self.create_importance_dict(new_join_graph, query, table_attr_dict)

        set_possible_queries, fp_on = self.FormulateQueries(final_edge_seq, query)
        self.remove_semantically_nonEq_queries(new_join_graph, query, set_possible_queries, fp_on)
        self.Q_E = self.sem_eq_queries[0] if len(self.sem_eq_queries) else None
        return True

    def create_final_edge_seq(self, list_of_tables, new_join_graph):
        final_edge_seq = []
        queue = []
        for tab in list_of_tables:
            edge_seq = []
            temp_njg = copy.deepcopy(new_join_graph)
            queue.append(tab)
            # table_t=tab
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
                    try:
                        temp_njg.remove(i)
                    except:
                        temp_njg.remove(list(reversed(i)))
                self.logger.debug(temp_njg)
            final_edge_seq.append(edge_seq)
        self.logger.debug("final_edge_seq: ", final_edge_seq)
        return final_edge_seq

    def create_importance_dict(self, new_join_graph, query, table_attr_dict):
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
            if len(res_hq) == 1:
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
            p_att_table1 = self.make_importance_dict_entry(key, edge[0][1], res_hq_dict, table_attr_dict)
            p_att_table2 = self.make_importance_dict_entry(key, edge[1][1], res_hq_dict, table_attr_dict)
            self.logger.debug(p_att_table1, p_att_table2)

        self.logger.debug(self.importance_dict)

    def make_importance_dict_entry(self, key, table, res_hq_dict, table_attr_dict):
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

    def create_table_attrib_dict(self):
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
        self.logger.debug("table_attr_dict: ", table_attr_dict)
        return table_attr_dict

    def get_tables_list_and_new_join_graph(self):
        new_join_graph = []
        list_of_tables = []
        for edge in self.global_join_graph:
            temp = []
            if len(edge) > 2:
                i = 0
                while i < (len(edge)) - 1:
                    temp = []
                    self.add_tabname_for_attrib(edge[i], list_of_tables, temp)
                    self.add_tabname_for_attrib(edge[i + 1], list_of_tables, temp)

                    i = i + 1
                    new_join_graph.append(temp)
            else:
                for vertex in edge:
                    self.add_tabname_for_attrib(vertex, list_of_tables, temp)
                new_join_graph.append(temp)
        self.logger.debug("list_of_tables: ", list_of_tables)
        self.logger.debug("new_join_graph: ", new_join_graph)
        return list_of_tables, new_join_graph

    def remove_semantically_nonEq_queries(self, new_join_graph, query, set_possible_queries, on_predicates):
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
                same = self.are_the_results_same(poss_q, query, same)
                self.update_with_val(attrib, table, s_val)
            for fp in on_predicates:
                attrib, tab = fp[1], fp[0]
                self.logger.debug(fp, attrib, tab)
                prev = self.get_dmin_val(attrib, tab)
                self.logger.debug(f"{tab}.{attrib} s_val {prev}, break val NULL")
                self.update_with_val(attrib, tab, 'NULL')
                same = self.are_the_results_same(poss_q, query, same)
                self.update_with_val(attrib, tab, prev)

            if same:
                sem_eq_queries.append(poss_q)

        self.sem_eq_queries = sem_eq_queries

    def are_the_results_same(self, poss_q, query, same):
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

    def add_tabname_for_attrib(self, attrib, list_of_tables, temp):
        tabname = self.find_tabname_for_given_attrib(attrib)
        if tabname not in list_of_tables:
            list_of_tables.append(tabname)
        self.logger.debug(attrib, tabname)
        temp.append((attrib, tabname))

    def update_attrib_to_see_impact(self, attrib: str, tabname: str) \
            -> Tuple[Union[int, float, date, str], Union[int, float, date, str]]:
        prev = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.select_attribs_from_relation([attrib], tabname), self.logger)
        val = 'NULL'
        self.logger.debug(f"update {tabname}.{attrib} with value {val} that had previous value {prev}")
        self.update_with_val(attrib, tabname, val)
        return val, prev

    def FormulateQueries(self, final_edge_seq, query):
        fp_on, fp_where = self.determine_on_and_where_filters(query)
        set_possible_queries = []
        # flat_list = [item for sublist in self.global_join_graph for item in sublist]
        # keys_of_tables = [*set(flat_list)]
        # tables_in_joins = [tab for tab in self.core_relations if
        #                   any(key in self.global_pk_dict[tab] for key in keys_of_tables)]
        # tables_not_in_joins = [tab for tab in self.core_relations if tab not in tables_in_joins]
        # self.logger.debug(tables_in_joins, tables_not_in_joins)

        for seq in final_edge_seq:
            # fp_on = copy.deepcopy(filter_pred_on)
            # fp_where = copy.deepcopy(filter_pred_where)
            # self.q_gen.from_op = ", ".join(tables_not_in_joins)
            flag_first = True
            # if len(tables_not_in_joins):
            #    flag_first = False
            for edge in seq:
                table1, table2 = edge[0][1], edge[1][1]
                imp_t1, imp_t2 = self.determine_join_edge_type(edge, table1, table2)
                flag_first = self.generate_from_on_clause(edge, flag_first, fp_on, imp_t1, imp_t2, table1, table2)
            self.generate_where_clause(fp_where)
            # assemble the rest of the query
            q_candidate = self.q_gen.generate_query()
            self.logger.debug("+++++++++++++++++++++")
            if q_candidate.count('OUTER'):
                set_possible_queries.append(q_candidate)

        for q in set_possible_queries:
            self.logger.debug(q)

        return set_possible_queries, fp_on

    def determine_on_and_where_filters(self, query):
        filter_pred_on = []
        filter_pred_where = []
        all_arithmetic_filters = self.q_gen.filter_predicates + self.q_gen.filter_in_predicates
        self.logger.debug("all_arithmetic_filters: ", all_arithmetic_filters)
        for fp in all_arithmetic_filters:
            self.logger.debug(f"fp from global filter predicates: {fp}")
            tab, attrib = fp[0], fp[1]
            _, prev = self.update_attrib_to_see_impact(attrib, tab)
            res_hq = self.app.doJob(query)
            self.logger.debug(f"res_hq: {res_hq}")
            if len(res_hq) == 1:
                filter_pred_where.append(fp)
            else:
                filter_pred_on.append(fp)
            self.update_with_val(attrib, tab, prev)
        self.logger.debug(filter_pred_on, filter_pred_where)
        return filter_pred_on, filter_pred_where

    def generate_where_clause(self, fp_where):
        self.q_gen.where_op = ''
        for elt in fp_where:
            self.add_where_clause(elt)

    def generate_from_on_clause(self, edge, flag_first, fp_on, imp_t1, imp_t2, table1, table2):
        if flag_first:
            self.q_gen.from_op = ''
        type_of_join = self.join_map.get((imp_t1, imp_t2))
        join_condition = f"\n\t ON {edge[0][1]}.{edge[0][0]} = {edge[1][1]}.{edge[1][0]}"
        relevant_tables = [table2] if not flag_first else [table1, table2]
        join_part = f"\n{type_of_join} {table2} {join_condition}"
        self.q_gen.from_op += f" {table1} {join_part}" if flag_first else "" + join_part
        flag_first = False
        for fp in fp_on:
            if fp[0] in relevant_tables:
                self.add_on_clause_for_filter(fp)
        return flag_first

    def determine_join_edge_type(self, edge, table1, table2):
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

    def add_where_clause(self, elt):
        predicate = self.q_gen.formulate_predicate_from_filter(elt)
        self.q_gen.where_op = predicate if self.q_gen.where_op == '' else self.q_gen.where_op + " and " + predicate

    def add_on_clause_for_filter(self, fp):
        predicate = self.q_gen.formulate_predicate_from_filter(fp)
        self.q_gen.from_op += "\n\t and " + predicate

    def extract_params_from_args(self, args):
        return args[0]
