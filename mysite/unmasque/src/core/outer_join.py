import copy

from ...src.core.abstract.AppExtractorBase import AppExtractorBase


class OuterJoin(AppExtractorBase):

    def __init__(self, connectionHelper, core_relations, join_graph):
        super().__init__(connectionHelper, "Outer Join")
        self.core_relations = core_relations
        self.join_graph = join_graph

    def doActualJob(self, args=None):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(['alter table ' + tabname + '_restore rename to ' + tabname + '2;',
                                               'drop table ' + tabname + ';',
                                               'alter table ' + tabname + '2 rename to ' + tabname + ';'])
            # The above command will inherently check if tabname1 exists

        # part of ---  def possible_edge_sequence():
        new_join_graph = []
        list_of_tables = []
        for edge in self.join_graph:
            temp = []
            if len(edge) > 2:
                i = 0
                while i < (len(edge)) - 1:
                    temp = []
                    tabname = self.connectionHelper.execute_sql_fetchone_0(
                        "SELECT COLUMN_NAME ,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME like '" + str(
                            edge[i]) + "' ;")
                    tn = tabname[1]
                    if tn not in list_of_tables:
                        list_of_tables.append(tabname[1])
                    print(edge[i], tabname)

                    temp.append(tabname)
                    tabname = self.connectionHelper.execute_sql_fetchone_0(
                        "SELECT COLUMN_NAME ,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME like '" + str(
                            edge[i + 1]) + "' ;")
                    tn = tabname[1]
                    print(tn)  #####
                    if tn not in list_of_tables:
                        list_of_tables.append(tn)
                    print(edge[i + 1], tabname)
                    temp.append(tabname)
                    i = i + 1
                    new_join_graph.append(temp)
            else:
                for vertex in edge:
                    tabname = self.connectionHelper.execute_sql_fetchone_0(
                        "SELECT COLUMN_NAME ,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME like '" + str(
                            vertex) + "' ;")
                    print(vertex, tabname)
                    temp.append(tabname)
                    tn = tabname[1]
                    print(tn)  #####
                    if tn not in list_of_tables:
                        list_of_tables.append(tn)
                new_join_graph.append(temp)

        print(list_of_tables)
        print(new_join_graph)

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
                print(temp_njg)
            final_edge_seq.append(edge_seq)
        print(final_edge_seq)

        # once dict is made compare values to null or not null
        # and prepare importance_dict

    def FormulateQueries(self, final_edge_seq):

        filter_pred_on = []
        filter_pred_where = []
        for fp in reveal_globals.global_filter_predicates:
            cur = reveal_globals.global_conn.cursor()
            # update partsupp set ps_partkey=(select ps_partkey from partsupp)+1;
            print("select " + fp[1] + " From " + fp[0] + ";")
            cur.execute("select " + fp[1] + " From " + fp[0] + ";")
            restore_value = cur.fetchall()
            cur.close()

            cur = reveal_globals.global_conn.cursor()
            # update partsupp set ps_partkey=(select ps_partkey from partsupp)+1;
            print("Update " + fp[0] + " Set " + fp[1] + " = Null;")
            cur.execute("Update " + fp[0] + " Set " + fp[1] + " = Null;")
            cur.close()
            cur = reveal_globals.global_conn.cursor()
            query = reveal_globals.query1
            cur.execute(query)
            res_hq = cur.fetchall()
            print(res_hq)
            cur.close()
            if len(res_hq) == 0:
                filter_pred_where.append(fp)
            else:
                filter_pred_on.append(fp)

            print(restore_value[0][0])
            cur = reveal_globals.global_conn.cursor()
            # update partsupp set ps_partkey=(select ps_partkey from partsupp)+1;
            print("Update " + fp[0] + " Set " + fp[1] + " = " + str(restore_value[0][0]) + ";")
            cur.execute("Update " + fp[0] + " Set " + fp[1] + " = " + str(restore_value[0][0]) + ";")
            cur.close()
        print(filter_pred_on, filter_pred_where)

        set_possible_queries = []
        ##formulate queries function using final_edge_seq
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

        flat_list = [item for sublist in self.join_graph for item in sublist]
        keys_of_tables = [*set(flat_list)]
        tables = reveal_globals.global_core_relations

        tables_in_joins = []
        for key in keys_of_tables:
            for tab in tables:
                if key in key_dict[tab] and tab not in tables_in_joins:
                    tables_in_joins.append(tab)

        tables_not_in_joins = []
        for tab in tables:
            if tab not in tables_in_joins:
                tables_not_in_joins.append(tab)

        print(tables_in_joins, tables_not_in_joins)

        importance_dict = reveal_globals.importance_dict

        for seq in final_edge_seq:

            fp_on = copy.deepcopy(filter_pred_on)
            fp_where = copy.deepcopy(filter_pred_where)

            query = "Select " + reveal_globals.global_select_op
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
                    print("error sneha!!!")

                print(imp_t1, imp_t2)
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
            reveal_globals.global_where_op = ''

            for elt in fp_where:
                predicate = ''
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
                if reveal_globals.global_where_op == '':
                    reveal_globals.global_where_op = predicate
                else:
                    reveal_globals.global_where_op = reveal_globals.global_where_op + " and " + predicate

            # assemble the rest of the query
            if reveal_globals.global_where_op != '':
                query = query + " Where " + reveal_globals.global_where_op
            if reveal_globals.global_groupby_op != '':
                query = query + " Group By " + reveal_globals.global_groupby_op
            if reveal_globals.global_orderby_op != '':
                query = query + " Order By " + reveal_globals.global_orderby_op
            if reveal_globals.global_limit_op != '':
                query = query + " Limit " + reveal_globals.global_limit_op

            print(query)
            print("+++++++++++++++++++++")
            set_possible_queries.append(query)

        return set_possible_queries

    def extract_params_from_args(self, args):
        pass
