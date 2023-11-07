from mysite.unmasque.refactored.equi_join import EquiJoin
from mysite.unmasque.src.core.QueryStringGenerator import generate_join_string


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
            return self.subqueries
        else:
            return self.global_join_graph

    def get_multiple_join_graphs(self):
        self.restore_d_min_from_dict_data()
        self.init_join_graph_dict()
        self.get_matching_tuples()
        self.do_traversal()

    def do_traversal(self):
        all_tabs = sorted(self.tab_tuple_sig_dict, key=lambda key: len(self.tab_tuple_sig_dict[key]), reverse=True)
        tab = all_tabs[0]
        tab_entry = self.tab_tuple_sig_dict[tab]
        for i in tab_entry.keys():
            entry = tab_entry[i]
            from_tabs = [tab, entry[1]]
            join_graph = [[entry[0], entry[3]]]
            self.subqueries.append((from_tabs, join_graph))

    def get_matching_tuples(self):
        for edge in self.global_join_graph:
            where_op = generate_join_string([edge])
            tabs = []
            ctids = []
            for e in edge:
                tab = self.find_tabname_for_given_attrib(e)
                tabs.append(tab)
                ctid = f"{tab}.ctid"
                ctids.append(ctid)
            from_op = ", ".join(tabs)
            select_op = ", ".join(ctids)
            ctid_query = f"select {select_op} from {from_op} where {where_op};"
            result_ctids, _ = self.connectionHelper.execute_sql_fetchall(ctid_query)
            for r in range(len(result_ctids)):
                result = result_ctids[r]
                for i in range(len(tabs) - 1):
                    self.tab_tuple_sig_dict[tabs[i]][result[i]] = (edge[i], tabs[i + 1], result[i + 1], edge[i + 1])
                for i in range(len(tabs) - 1, 0, -1):
                    self.tab_tuple_sig_dict[tabs[i]][result[i]] = (edge[i], tabs[i - 1], result[i - 1], edge[i - 1])

    def init_join_graph_dict(self):
        for key_tab in self.global_min_instance_dict:
            self.tab_tuple_sig_dict[key_tab] = {}  # each table entry is a dict
