import copy

from mysite.unmasque.refactored.equi_join import EquiJoin
from mysite.unmasque.refactored.util.common_queries import alter_table_rename_to, get_tabname_2, drop_view


class MultipleEquiJoin(EquiJoin):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_min_instance_dict):
        super().__init__(connectionHelper,
                         global_key_lists,
                         core_relations,
                         global_min_instance_dict)
        self.join_key_subquery_dict = {}
        self.global_all_join_graphs = []
        self.intersection = False
        self.intersection_flag = False
        self.from_clauses = {}
        self.join_key_vs_tab_dict = {}
        self.key_vals_in_tab_dict = {}

    def is_intersection_present(self):
        if not self.intersection_flag:
            self.intersection = any(len(value) > 2 for value in self.global_min_instance_dict.values())
            self.intersection_flag = True
        return self.intersection

    def get_join_graph(self, query):
        super().get_join_graph(query)

        self.prepare_subquery_key_dict()
        if self.is_intersection_present():
            self.get_multiple_join_graphs()
            return self.global_all_join_graphs
        else:
            return self.global_join_graph

    def get_multiple_join_graphs(self):

        dict_working_copy = copy.deepcopy(self.join_key_subquery_dict)

        simple_join_dict = {}
        for edge in self.global_join_graph:
            for key in edge:
                simple_join_dict[key] = 1

        join_edges = self.global_join_graph
        edges_remain = True
        while edges_remain:
            this_join_graph = []
            for edge in join_edges:
                use_edge = False
                for key in edge:
                    if dict_working_copy[key] > 0:
                        dict_working_copy[key] -= 1
                        use_edge = True
                if use_edge:
                    this_join_graph.append(edge)
                edges_remain = any(dict_working_copy.values())
            if this_join_graph:
                self.global_all_join_graphs.append(this_join_graph)

    def prepare_subquery_key_dict(self):
        join_edges = self.global_join_graph
        for edge in join_edges:
            for key in edge:
                edge_copy = copy.deepcopy(edge)
                edge_copy.remove(key)
                tab = self.find_tabname_for_given_attrib(key)
                self.join_key_vs_tab_dict[key] = tab
                self.update_from_clause_for_join_edge(edge, tab)
                res, desc = self.connectionHelper.execute_sql_fetchall(f"select {key} from {tab}")
                self.save_value(key, res)
                self.join_key_subquery_dict[key] = len(res)
                self.adjust_subquery_count_for_join_keys(edge_copy, key)

    def save_value(self, key, res):
        value = []
        for row in res:
            if row[0] not in value:
                value.append(row[0])
        self.key_vals_in_tab_dict[key] = value

    def adjust_subquery_count_for_join_keys(self, edge_copy, key):
        for other_key in edge_copy:
            if other_key in self.join_key_subquery_dict.keys():
                if self.join_key_subquery_dict[other_key] < self.join_key_subquery_dict[key]:
                    self.join_key_subquery_dict[key] = self.join_key_subquery_dict[other_key]
                if self.join_key_subquery_dict[key] < self.join_key_subquery_dict[other_key]:
                    self.join_key_subquery_dict[other_key] = self.join_key_subquery_dict[key]

    def update_from_clause_for_join_edge(self, edge, tab):
        key = hash(frozenset(edge))
        if key in self.from_clauses.keys():
            self.from_clauses[key].append(tab)
        else:
            self.from_clauses[key] = [tab]

    def validate_global_all_join_graphs(self, query):
        all_tabs_dict = {}
        for join_graph in self.global_all_join_graphs:
            per_tab_dict = {}

            for join_edge in join_graph:
                for join_key in join_edge:
                    key_val = self.key_vals_in_tab_dict[join_key][0]
                    tab = self.join_key_vs_tab_dict[join_key]
                    if tab not in per_tab_dict.keys():
                        per_tab_dict[tab] = [f"{join_key} = {key_val}"]
                    else:
                        per_tab_dict[tab].append(f"{join_key} = {key_val}")

            for tab in per_tab_dict.keys():
                key_vals = per_tab_dict[tab]
                where_keys_are = " or ".join(key_vals)

                if tab not in all_tabs_dict.keys():
                    all_tabs_dict[tab] = [f"(select * from {get_tabname_2(tab)} where {where_keys_are})"]
                else:
                    all_tabs_dict[tab].append(f"(select * from {get_tabname_2(tab)} where {where_keys_are})")

        for tab in all_tabs_dict.keys():
            where = " UNION ".join(all_tabs_dict[tab])
            self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_2(tab)),
                                               f"create view {tab} as {where};"])
        new_result = self.app.doJob(query)
        if len(new_result) > 1:
            valid = True
        else:
            valid = False

        for tab in all_tabs_dict.keys():
            self.connectionHelper.execute_sql([drop_view(tab), alter_table_rename_to(get_tabname_2(tab), tab)])
        return valid

