import copy

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase


class OuterJoin(GenerationPipeLineBase):

    def __init__(self, connectionHelper, delivery, projected_attributes):
        super().__init__(connectionHelper, "Outer Join", delivery)
        self.importance_dict = {}
        self.projected_attributes = projected_attributes
        self.Q_E = None

    def doExtractJob(self, query: str) -> bool:
        list_of_tables, new_join_graph = self.get_tables_list_and_new_join_graph()
        final_edge_seq = self.create_final_edge_seq(list_of_tables, new_join_graph)
        table_attr_dict = self.create_table_attrib_dict()
        return True

        # self.restore_d_min()
        self.create_importance_dict(new_join_graph, query, table_attr_dict)
        set_possible_queries = self.FormulateQueries(final_edge_seq, query)
        sem_eq_queries = self.remove_semantically_nonEq_queries(query, set_possible_queries)
        self.Q_E = sem_eq_queries[0]
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
        importance_dict = {}
        for edge in new_join_graph:
            # modify d1
            # break join condition on this edge
            tuple1 = edge[0]
            key = tuple1[0]
            table = tuple1[1]
            self.connectionHelper.execute_sql(
                ['Update ' + table + ' set ' + key + '= -(select ' + key + ' from ' + table + ');'])

            res_hq = self.app.doJob(query)
            self.logger.debug(res_hq)

            self.connectionHelper.execute_sql(
                ['Update ' + table + ' set ' + key + '= -(select ' + key + ' from ' + table + ');'])

            # make dict of table, attributes: projected val
            loc = {}
            for table in table_attr_dict.keys():
                loc[table_attr_dict[table]] = colnames.index(table_attr_dict[table])
            self.logger.debug(loc)

            res_hq_dict = {}
            if len(res_hq) == 0:
                for k in loc.keys():
                    if k not in res_hq_dict.keys():
                        res_hq_dict[k] = [None]
                    else:
                        res_hq_dict[k].append(None)
            else:
                for l in range(len(res_hq)):
                    for k in loc.keys():
                        if k not in res_hq_dict.keys():
                            res_hq_dict[k] = [res_hq[l][loc[k]]]
                        else:
                            res_hq_dict[k].append(res_hq[l][loc[k]])
            self.logger.debug(res_hq_dict)

            # importance_dict={}
            # for e in new_join_graph:
            importance_dict[tuple(edge)] = {}
            table1 = edge[0][1]  # first table
            table2 = edge[1][1]

            p_att_table1 = None
            p_att_table2 = None
            att1 = table_attr_dict[table1]
            att2 = table_attr_dict[table2]
            if att1 in res_hq_dict.keys():
                for i in res_hq_dict[att1]:
                    if i is not None:
                        p_att_table1 = 10
                        break
            if att2 in res_hq_dict.keys():
                for i in res_hq_dict[att2]:
                    if i is not None:
                        p_att_table2 = 10
                        break

            self.logger.debug(p_att_table1, p_att_table2)
            temp1 = ''
            temp2 = ''
            if p_att_table1 is None and p_att_table2 is None:
                temp1 = 'l'
                temp2 = 'l'
            elif p_att_table1 is None and p_att_table2 is not None:
                temp1 = 'l'
                temp2 = 'h'
            elif p_att_table1 is not None and p_att_table2 is None:
                temp1 = 'h'
                temp2 = 'l'
            elif p_att_table1 is not None and p_att_table2 is not None:
                temp1 = 'h'
                temp2 = 'h'

            importance_dict[tuple(edge)][table1] = temp1
            importance_dict[tuple(edge)][table2] = temp2
        self.logger.debug(importance_dict)
        self.importance_dict = importance_dict

    def create_table_attrib_dict(self):
        # once dict is made compare values to null or not null
        # and prepare importance_dict
        table_attr_dict = {}
        # for each table find a projected attribute which we will check for null values
        for k in self.projected_attributes:
            tabname = self.find_tabname_for_given_attrib(k)
            if tabname not in table_attr_dict.keys():
                self.logger.debug(k, tabname)
                table_attr_dict[tabname] = k
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

    def restore_d_min(self):
        # preparing D_1
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(['alter table ' + tabname + ' rename to ' + tabname + '_restore;',
                                               'create table ' + tabname + ' as select * from ' + tabname + '4;'])

    def backup_relations(self):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(['alter table ' + tabname + '_restore rename to ' + tabname + '2;',
                                               'drop table ' + tabname + ';',
                                               'alter table ' + tabname + '2 rename to ' + tabname + ';'])
            # The above command will inherently check if tabname1 exists

    def restore_relations(self):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(['drop table ' + tabname + ';',
                                               'alter table ' + tabname + '_restore rename to ' + tabname + ';'])

    def remove_semantically_nonEq_queries(self, query, set_possible_queries):
        # eliminate semanticamy non-equivalent querie from set_possible_queries
        # this code needs to be finished (27 feb)
        sem_eq_queries = []
        res_HQ = self.app.doJob(query)
        # self.logger.debug(res_HQ)
        for poss_q in set_possible_queries:
            res_poss_q = self.app.doJob(poss_q)
            # self.logger.debug(res_poss_q)

            if len(res_HQ) != len(res_poss_q):
                pass
            else:
                same = 1
                for var in range(len(res_HQ)):
                    self.logger.debug(res_HQ[var] == res_poss_q[var])
                    if not (res_HQ[var] == res_poss_q[var]):
                        same = 0

                if same == 1:
                    sem_eq_queries.append(poss_q)
        return sem_eq_queries

    def add_tabname_for_attrib(self, attrib, list_of_tables, temp):
        tabname = self.find_tabname_for_given_attrib(attrib)
        if tabname not in list_of_tables:
            list_of_tables.append(tabname)
        self.logger.debug(attrib, tabname)
        temp.append(tabname)

    def FormulateQueries(self, final_edge_seq, query):

        filter_pred_on = []
        filter_pred_where = []
        for fp in self.global_filter_predicates:
            restore_value = self.connectionHelper.execute_sql_fetchall("select " + fp[1] + " From " + fp[0] + ";")
            self.connectionHelper.execute_sql(["Update " + fp[0] + " Set " + fp[1] + " = Null;"])
            res_hq = self.app.doJob(query)
            if len(res_hq) == 0:
                filter_pred_where.append(fp)
            else:
                filter_pred_on.append(fp)

            self.logger.debug(restore_value[0][0])
            self.connectionHelper.execute_sql(
                ["Update " + fp[0] + " Set " + fp[1] + " = " + str(restore_value[0][0]) + ";"])
        self.logger.debug(filter_pred_on, filter_pred_where)

        set_possible_queries = []
        key_dict = {
            'nation': ['n_nationkey', 'n_regionkey'],
            'region': ['r_regionkey'],
            'supplier': ['s_suppkey', 's_nationkey'],
            'partsupp': ['ps_partkey', 'ps_suppkey'],
            'part': ['p_partkey'],
            'lineitem': ['l_orderkey', 'l_partkey', 'l_suppkey'],
            'orders': ['o_orderkey', 'o_custkey'],
            'customer': ['c_custkey', 'c_nationkey']
        }

        flat_list = [item for sublist in self.global_join_graph for item in sublist]
        keys_of_tables = [*set(flat_list)]
        tables = self.core_relations

        tables_in_joins = []
        for key in keys_of_tables:
            for tab in tables:
                if key in key_dict[tab] and tab not in tables_in_joins:
                    tables_in_joins.append(tab)

        tables_not_in_joins = []
        for tab in tables:
            if tab not in tables_in_joins:
                tables_not_in_joins.append(tab)

        self.logger.debug(tables_in_joins, tables_not_in_joins)

        importance_dict = self.importance_dict

        for seq in final_edge_seq:

            fp_on = copy.deepcopy(filter_pred_on)
            fp_where = copy.deepcopy(filter_pred_where)

            query = "Select " + self.select_op
            # handle tables not participating in join
            if len(tables_not_in_joins) != 0:
                query += " From "
                for tab in tables_not_in_joins:
                    query += tab + " , "
            else:
                query += " From "

            flag_first = True
            for edge in seq:
                # steps to determine type of join for edge
                if tuple(edge) in importance_dict.keys():
                    imp_t1 = importance_dict[tuple(edge)][edge[0][1]]
                    imp_t2 = importance_dict[tuple(edge)][edge[1][1]]
                elif tuple(list(reversed(edge))) in importance_dict.keys():
                    imp_t1 = importance_dict[tuple(list(reversed(edge)))][edge[0][1]]
                    imp_t2 = importance_dict[tuple(list(reversed(edge)))][edge[1][1]]
                else:
                    self.logger.debug("error sneha!!!")

                self.logger.debug(imp_t1, imp_t2)
                if imp_t1 == 'l' and imp_t2 == 'l':
                    type_of_join = ' Inner Join '
                elif imp_t1 == 'l' and imp_t2 == 'h':
                    type_of_join = ' Right Outer Join '
                elif imp_t1 == 'h' and imp_t2 == 'l':
                    type_of_join = ' Left Outer Join '
                elif imp_t1 == 'h' and imp_t2 == 'h':
                    type_of_join = ' Full Outer Join '

                if flag_first:
                    query += str(edge[0][1]) + type_of_join + str(edge[1][1]) + ' ON ' + str(
                        edge[0][0]) + ' = ' + str(edge[1][0])
                    flag_first = False
                    # check for filter predicates for both tables
                    # append fp to query
                    table1 = edge[0][1]
                    table2 = edge[1][1]
                    for fp in fp_on:
                        if fp[0] == table1 or fp[0] == table2:
                            predicate = ''
                            elt = fp
                            if elt[2].strip() == 'range':
                                if '-' in str(elt[4]):
                                    predicate = elt[1] + " between " + str(elt[3]) + " and " + str(elt[4])
                                else:
                                    predicate = elt[1] + " between " + " '" + str(
                                        elt[3]) + "'" + " and " + " '" + str(elt[4]) + "'"
                            elif elt[2].strip() == '>=':
                                if '-' in str(elt[3]):
                                    predicate = elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "
                                else:
                                    predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[3])
                            elif 'equal' in elt[2] or 'like' in elt[2].lower() or '-' in str(elt[4]):
                                predicate = elt[1] + " " + str(elt[2]).replace('equal', '=') + " '" + str(
                                    elt[4]) + "'"
                            else:
                                predicate = elt[1] + ' ' + str(elt[2]) + ' ' + str(elt[4])
                            query += " and " + predicate
                            # fp_on.remove(fp)

                else:
                    query += ' ' + type_of_join + str(edge[1][1]) + ' ON ' + str(edge[0][0]) + ' = ' + str(
                        edge[1][0])
                    # check for filter predicates for second tables
                    # append fp to query

                    for fp in fp_on:
                        if fp[0] == edge[1][1]:
                            predicate = ''
                            elt = fp
                            if elt[2].strip() == 'range':
                                if '-' in str(elt[4]):
                                    predicate = elt[1] + " between " + str(elt[3]) + " and " + str(elt[4])
                                else:
                                    predicate = elt[1] + " between " + " '" + str(
                                        elt[3]) + "'" + " and " + " '" + str(elt[4]) + "'"
                            elif elt[2].strip() == '>=':
                                if '-' in str(elt[3]):
                                    predicate = elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "
                                else:
                                    predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[3])
                            elif 'equal' in elt[2] or 'like' in elt[2].lower() or '-' in str(elt[4]):
                                predicate = elt[1] + " " + str(elt[2]).replace('equal', '=') + " '" + str(
                                    elt[4]) + "'"
                            else:
                                predicate = elt[1] + ' ' + str(elt[2]) + ' ' + str(elt[4])
                            query += " and " + predicate
                            # fp_on.remove(fp)
            # add other components of the query
            # + where clause
            # + group by, order by, limit
            self.where_op = ''

            for elt in fp_where:
                if elt[2].strip() == 'range':
                    if '-' in str(elt[4]):
                        predicate = elt[1] + " between " + str(elt[3]) + " and " + str(elt[4])
                    else:
                        predicate = elt[1] + " between " + " '" + str(elt[3]) + "'" + " and " + " '" + str(
                            elt[4]) + "'"
                elif elt[2].strip() == '>=':
                    if '-' in str(elt[3]):
                        predicate = elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "
                    else:
                        predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[3])
                elif 'equal' in elt[2] or 'like' in elt[2].lower() or '-' in str(elt[4]):
                    predicate = elt[1] + " " + str(elt[2]).replace('equal', '=') + " '" + str(elt[4]) + "'"
                else:
                    predicate = elt[1] + ' ' + str(elt[2]) + ' ' + str(elt[4])
                if self.where_op == '':
                    self.where_op = predicate
                else:
                    self.where_op = self.where_op + " and " + predicate

            # assemble the rest of the query
            if self.where_op != '':
                query = query + " Where " + self.where_op
            if self.groupby_op != '':
                query = query + " Group By " + self.groupby_op
            if self.orderby_op != '':
                query = query + " Order By " + self.orderby_op
            if self.limit_op != '':
                query = query + " Limit " + self.limit_op

            self.logger.debug(query)
            self.logger.debug("+++++++++++++++++++++")
            set_possible_queries.append(query)

        return set_possible_queries

    def extract_params_from_args(self, args):
        return args[0]
