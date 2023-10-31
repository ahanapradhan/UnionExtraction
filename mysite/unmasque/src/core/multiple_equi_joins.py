from mysite.unmasque.refactored.equi_join import EquiJoin


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

    def get_join_graph(self, query):
        super().get_join_graph(query)
        self.restore_d_min()
        join_edges = self.global_join_graph
        for edge in join_edges:
            for key in edge:
                tab = self.find_tabname_for_given_attrib(key)
                res, desc = self.connectionHelper.execute_sql_fetchall(f"select {key} from {tab}")
                self.logger.info(f"key {key} is present in {len(res)} subqueries")
                self.join_key_subquery_dict[key] = len(res)
