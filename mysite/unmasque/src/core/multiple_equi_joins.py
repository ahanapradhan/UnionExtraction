from mysite.unmasque.refactored.equi_join import EquiJoin
from mysite.unmasque.src.core.abstract.ExtractorModuleBase import ExtractorModuleBase
from mysite.unmasque.src.core.abstract.dataclass.from_clause_data_class import FromData
from mysite.unmasque.src.core.abstract.dataclass.join_data_class import JoinData
from mysite.unmasque.src.core.abstract.spj_QueryStringGenerator import generate_join_string


def create_token(row_ctid, tab):
    token = str(tab) + "+" + str(row_ctid)
    return token


def check_and_add(tab, from_tabs, join_graph, next_key, next_tab, this_key, token, visited):
    this_join = [this_key, next_key]
    that_join = this_join[::-1]
    if tab not in from_tabs:
        from_tabs.append(tab)
    if next_tab not in from_tabs:
        from_tabs.append(next_tab)
    if this_join not in join_graph and that_join not in join_graph:
        join_graph.append(this_join)
    visited.append(token)


def form_global_key_attributes(join_graph):
    key_attributes = []
    for edge in join_graph:
        for e in edge:
            if e not in key_attributes:
                key_attributes.append(e)
    return key_attributes


class MultipleEquiJoin(ExtractorModuleBase):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_min_instance_dict):
        super().__init__(connectionHelper, "Multiple Equi Join")
        self.fromData = []
        self.joinData = []
        self.intersection = False
        self.intersection_flag = False
        self.tab_tuple_sig_dict = {}
        self.join_extractor = EquiJoin(self.connectionHelper, global_key_lists, core_relations, global_min_instance_dict)

    def is_intersection_present(self):
        if not self.intersection_flag:
            self.intersection = any(len(value) > 2 for value in self.join_extractor.global_min_instance_dict.values())
            self.intersection_flag = True
        return self.intersection

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        check = self.join_extractor.doJob(query)
        if not check:
            return False
        if self.is_intersection_present():
            self.get_multiple_join_graphs()
        else:
            self.fill_in_data_fields(self.join_extractor.core_relations, self.join_extractor.global_join_graph)
        return True

    def get_multiple_join_graphs(self):
        self.join_extractor.restore_d_min_from_dict_data()
        self.get_matching_tuples()
        self.do_traversal()

    def do_traversal(self):
        all_tabs = sorted(self.tab_tuple_sig_dict, key=lambda key: len(self.tab_tuple_sig_dict[key]), reverse=True)
        tab = all_tabs[0]
        tab_entry = self.tab_tuple_sig_dict[tab]
        for row_ctid in tab_entry.keys():
            from_tabs, join_graph = self.traverse_for_one_cycle(row_ctid, tab)
            self.fill_in_data_fields(from_tabs, join_graph)

    def fill_in_data_fields(self, from_tabs, join_graph):
        from_data = FromData()
        join_data = JoinData()
        from_data.core_relations = from_tabs
        join_data.global_join_graph = join_graph
        join_data.global_key_attributes = form_global_key_attributes(join_graph)
        self.fromData.append(from_data)
        self.joinData.append(join_data)

    def traverse_for_one_cycle(self, row_ctid, tab):
        join_graph = []
        from_tabs = []
        visited = []
        while True:
            token = create_token(row_ctid, tab)
            if token in visited:
                break
            entry = self.tab_tuple_sig_dict[tab][row_ctid]
            this_key, next_tab, next_ctid, next_key = entry[0], entry[1], entry[2], entry[3]
            check_and_add(tab, from_tabs, join_graph, next_key, next_tab, this_key, token, visited)
            tab, row_ctid = next_tab, next_ctid

        for key_tab in self.tab_tuple_sig_dict.keys():
            for key_ctid in self.tab_tuple_sig_dict[key_tab].keys():
                token = create_token(key_ctid, key_tab)
                entry = self.tab_tuple_sig_dict[key_tab][key_ctid]
                this_key, next_tab, next_ctid, next_key = entry[0], entry[1], entry[2], entry[3]
                next_token = create_token(next_ctid, next_tab)
                if token not in visited and next_token in visited:
                    check_and_add(key_tab, from_tabs, join_graph, next_key, next_tab, this_key, token, visited)

        return from_tabs, join_graph

    def get_matching_tuples(self):
        self.tab_tuple_sig_dict = {key_tab: {} for key_tab in self.join_extractor.global_min_instance_dict}

        for edge in self.join_extractor.global_join_graph:
            tabs = [self.join_extractor.find_tabname_for_given_attrib(e) for e in edge]
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
