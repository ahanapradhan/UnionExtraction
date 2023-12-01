import copy

import pandas as pd

from mysite.unmasque.refactored.equi_join import EquiJoin
from mysite.unmasque.src.core.abstract.ExtractorModuleBase import ExtractorModuleBase
from mysite.unmasque.src.core.dataclass.SubQueryData import SubQueryData
from mysite.unmasque.src.core.abstract.spj_QueryStringGenerator import generate_join_string


def create_token(row_ctid, tab):
    token = str(tab) + "+" + str(row_ctid)
    return token


def check_and_add(tab, this_key, this_ctid, next_tab, next_key, next_ctid,
                  from_tabs, join_graph, token, d_min_dict, visited):
    this_join = [this_key, next_key]
    that_join = this_join[::-1]
    if tab not in from_tabs:
        from_tabs.append(tab)
    if next_tab not in from_tabs:
        from_tabs.append(next_tab)
    if this_join not in join_graph and that_join not in join_graph:
        join_graph.append(this_join)
    d_min_dict[tab] = this_ctid
    d_min_dict[next_tab] = next_ctid
    visited.append(token)


def form_global_key_attributes(join_graph):
    key_attributes = []
    for edge in join_graph:
        for e in edge:
            if e not in key_attributes:
                key_attributes.append(e)
    return key_attributes


class ManyEquiJoin(ExtractorModuleBase):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_min_instance_dict):
        super().__init__(connectionHelper, "Multiple Equi Join")
        self.subquery_data = []
        self.intersection = False
        self.intersection_flag = False
        self.tab_tuple_sig_dict = {}
        self.joinEdge_extractor = EquiJoin(self.connectionHelper, global_key_lists,
                                           core_relations, global_min_instance_dict)

    def is_intersection_present(self):
        if not self.intersection_flag:
            self.intersection = any(len(value) > 2 for value in self.joinEdge_extractor.global_min_instance_dict.values())
            self.intersection_flag = True
        return self.intersection

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        check = self.joinEdge_extractor.doJob(query)
        if not check:
            return False
        self.get_actual_join_data()
        self.set_aux_data()
        return True

    def get_actual_join_data(self):
        if self.is_intersection_present():
            self.get_multiple_join_graphs()
        else:
            self.get_only_join_graph()

    def set_aux_data(self):
        for s in self.subquery_data:
            s.equi_join.global_attrib_types = copy.deepcopy(self.joinEdge_extractor.global_attrib_types)

    def get_only_join_graph(self):
        default_d_min_dict = {}
        for table in self.joinEdge_extractor.core_relations:
            default_d_min_dict[table] = '(0,1)'
        self.fill_in_data_fields(self.joinEdge_extractor.core_relations, self.joinEdge_extractor.global_join_graph,
                                 default_d_min_dict)

    def get_multiple_join_graphs(self):
        self.joinEdge_extractor.restore_d_min_from_dict_data()
        self.get_matching_tuples()
        self.do_traversal()

    def do_traversal(self):
        all_tabs = sorted(self.tab_tuple_sig_dict, key=lambda key: len(self.tab_tuple_sig_dict[key]), reverse=True)
        tab = all_tabs[0]
        tab_entry = self.tab_tuple_sig_dict[tab]
        for row_ctid in tab_entry.keys():
            from_tabs, join_graph, d_min_dict = self.traverse_for_one_cycle(row_ctid, tab)
            self.fill_in_data_fields(from_tabs, join_graph, d_min_dict)

    def fill_in_data_fields(self, from_tabs, join_graph, d_min_dict):
        subquery = SubQueryData()
        subquery.from_clause.core_relations = from_tabs
        subquery.equi_join.global_join_graph = join_graph
        subquery.equi_join.global_key_attributes = form_global_key_attributes(join_graph)
        subquery.d_min_dict = self.populate_min_instance_dict(d_min_dict)
        for tab in from_tabs:
            i = self.joinEdge_extractor.core_relations.index(tab)
            tab_attribs = self.joinEdge_extractor.global_all_attribs[i]
            subquery.equi_join.global_all_attribs.append(tab_attribs)
        self.subquery_data.append(subquery)

    def populate_min_instance_dict(self, d_min_dict):
        data_dict = {}
        for key_tab in d_min_dict.keys():
            data_dict[key_tab] = []
            sql_query = pd.read_sql_query(f"select * from {key_tab} where ctid = '{d_min_dict[key_tab]}';",
                                          self.connectionHelper.conn)
            df = pd.DataFrame(sql_query)
            data_dict[key_tab].append(tuple(df.columns))
            for index, row in df.iterrows():
                data_dict[key_tab].append(tuple(row))
        return data_dict

    def traverse_for_one_cycle(self, row_ctid, tab):
        join_graph = []
        from_tabs = []
        d_min_dict = {}
        visited = []
        while True:
            token = create_token(row_ctid, tab)
            if token in visited:
                break
            entry = self.tab_tuple_sig_dict[tab][row_ctid]
            this_key, next_tab, next_ctid, next_key = entry[0], entry[1], entry[2], entry[3]
            check_and_add(tab, this_key, row_ctid, next_tab, next_key, next_ctid,
                          from_tabs, join_graph, token, d_min_dict, visited)
            tab, row_ctid = next_tab, next_ctid

        for key_tab in self.tab_tuple_sig_dict.keys():
            for key_ctid in self.tab_tuple_sig_dict[key_tab].keys():
                token = create_token(key_ctid, key_tab)
                entry = self.tab_tuple_sig_dict[key_tab][key_ctid]
                this_key, next_tab, next_ctid, next_key = entry[0], entry[1], entry[2], entry[3]
                next_token = create_token(next_ctid, next_tab)
                if token not in visited and next_token in visited:
                    check_and_add(key_tab, this_key, key_ctid, next_tab, next_key, next_ctid,
                                  from_tabs, join_graph, token, d_min_dict, visited)

        return from_tabs, join_graph, d_min_dict

    def get_matching_tuples(self):
        self.tab_tuple_sig_dict = {key_tab: {} for key_tab in self.joinEdge_extractor.global_min_instance_dict}

        for edge in self.joinEdge_extractor.global_join_graph:
            tabs = [self.joinEdge_extractor.find_tabname_for_given_attrib(e) for e in edge]
            ctids = [f"{tab}.ctid" for tab in tabs]
            from_op = ", ".join(tabs)
            select_op = ", ".join(ctids)
            where_op = generate_join_string([edge])
            ctid_query = f"select {select_op} from {from_op} where {where_op};"
            result_ctids, _ = self.connectionHelper.execute_sql_fetchall(ctid_query)
            self.add_to_sig_dict(edge, result_ctids, tabs)

    def add_to_sig_dict(self, edge, result_ctids, tabs):
        for r in range(len(result_ctids)):
            result = result_ctids[r]
            for i in range(len(tabs) - 1):
                self.tab_tuple_sig_dict[tabs[i]][result[i]] = (edge[i], tabs[i + 1], result[i + 1], edge[i + 1])
            for i in range(len(tabs) - 1, 0, -1):
                self.tab_tuple_sig_dict[tabs[i]][result[i]] = (edge[i], tabs[i - 1], result[i - 1], edge[i - 1])
