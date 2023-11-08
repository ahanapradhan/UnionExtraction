from mysite.unmasque.refactored.equi_join import EquiJoin
from mysite.unmasque.src.core.abstract.spj_QueryStringGenerator import generate_join_string


class MultipleEquiJoin(EquiJoin):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_min_instance_dict):
        super().__init__(connectionHelper,
                         global_key_lists,
                         core_relations,
                         global_min_instance_dict)
        self.subqueries = []
        self.intersection = False
        self.intersection_flag = False
        self.tab_tuple_sig_dict = {}

    def is_intersection_present(self):
        if not self.intersection_flag:
            self.intersection = any(len(value) > 2 for value in self.global_min_instance_dict.values())
            self.intersection_flag = True
        return self.intersection

    def get_join_graph(self, query):
        super().get_join_graph(query)
        if self.is_intersection_present():
            self.get_multiple_join_graphs()
            self.logger.debug(self.subqueries)
            return self.subqueries
        else:
            self.logger.debug(self.global_join_graph)
            return self.global_join_graph

    def get_multiple_join_graphs(self):
        self.restore_d_min_from_dict_data()
        self.get_matching_tuples()
        self.do_traversal()

    def do_traversal(self):
        all_tabs = sorted(self.tab_tuple_sig_dict, key=lambda key: len(self.tab_tuple_sig_dict[key]), reverse=True)
        tab = all_tabs[0]
        tab_entry = self.tab_tuple_sig_dict[tab]
        for row_ctid in tab_entry.keys():
            from_tabs, join_graph = self.traverse_for_one_cycle(row_ctid, tab)
            self.subqueries.append((from_tabs, join_graph))

    def traverse_for_one_cycle(self, row_ctid, tab):

        join_graph = []
        from_tabs = [tab]
        visited = []

        while True:
            token = str(tab) + "+" + str(row_ctid)
            if token in visited:
                break

            entry = self.tab_tuple_sig_dict[tab][row_ctid]
            this_key, next_tab, next_key, next_ctid = entry[0], entry[1], entry[3], entry[2]

            this_join = [this_key, next_key]
            that_join = this_join[::-1]

            if next_tab not in from_tabs:
                from_tabs.append(next_tab)
            if this_join not in join_graph and that_join not in join_graph:
                join_graph.append(this_join)

            visited.append(token)

            tab, row_ctid = next_tab, next_ctid

        return from_tabs, join_graph

    def get_matching_tuples(self):
        self.tab_tuple_sig_dict = {key_tab: {} for key_tab in self.global_min_instance_dict}

        for edge in self.global_join_graph:
            tabs = [self.find_tabname_for_given_attrib(e) for e in edge]
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
