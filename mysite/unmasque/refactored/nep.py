import ast

from .abstract.AfterWhereClauseExtractorBase import AfterWhereClauseBase
from .abstract.MinimizerBase import Minimizer
from .util.common_queries import get_restore_name
from .util.utils import get_dummy_val_for, get_format, get_char


class NEP(Minimizer, AfterWhereClauseBase):

    def __init__(self, connectionHelper, core_relations, all_sizes,
                 global_pk_dict,
                 global_all_attribs,
                 global_attrib_types,
                 filter_predicates,
                 global_key_attributes,
                 query_generator):
        Minimizer.__init__(self, connectionHelper, core_relations, all_sizes, "NEP")
        AfterWhereClauseBase.__init__(self, connectionHelper, "NEP",
                                      core_relations,
                                      global_all_attribs,
                                      global_attrib_types,
                                      None,
                                      filter_predicates)
        self.Q_E = ""
        self.global_pk_dict = global_pk_dict  # from initialization
        self.global_key_attributes = global_key_attributes
        self.query_generator = query_generator

    def extract_params_from_args(self, args):
        print(args)
        tup = args[0]
        q_h = tup[0]
        q_e = tup[1]
        return q_h, q_e

    def doActualJob(self, args):
        query, Q_E = self.extract_params_from_args(args)
        core_sizes = self.getCoreSizes()

        # STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
        partition_dict = {}
        for key in core_sizes.keys():
            partition_dict[key] = (0, core_sizes[key])

        self.create_all_views()

        # Run the hidden query on the original database instance
        new_result = self.app.doJob(query)
        if not self.match(Q_E, new_result):
            # NEP may exists
            print("NEP may exists")
            for i in range(len(self.core_relations)):
                tabname = self.core_relations[i]
                Q_E_ = self.nep_db_minimizer(query, tabname, Q_E, core_sizes[tabname], partition_dict[tabname], i)
                self.Q_E = Q_E_
                # Run the hidden query on the original database instance
                new_result = self.app.doJob(query)
                if self.match(Q_E, new_result):
                    break

            nep_exists = True
        else:
            nep_exists = False
            self.Q_E = Q_E
            print("NEP doesn't exists under our assumptions")

        self.drop_all_views()
        return nep_exists

    def create_all_views(self):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(["drop table " + tabname + ";",
                                               "create view " + tabname + " as select * from "
                                               + get_restore_name(tabname) + " ;"])

    def drop_all_views(self):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(["alter view " + tabname + " rename to " + tabname + "3;",
                                               "create table " + tabname + " as select * from " + tabname + "3;",
                                               "drop view " + tabname + "3;"])

    def match(self, Q_E, new_result):
        # Run the extracted query Q_E .
        self.connectionHelper.execute_sql(["create view temp1 as " + Q_E])

        # Size of the table
        res = self.connectionHelper.execute_sql_fetchone_0("select count(*) from temp1;")

        if res < 5000:
            # Drop the temporary table and view created.
            self.connectionHelper.execute_sql(["drop view temp1;"])
            return self.match1(Q_E, new_result)

        # Create an empty table with name temp2
        self.connectionHelper.execute_sql(["Create unlogged table temp2 (like temp1);"])

        # Header of temp2
        t1 = f'({", ".join(new_result[0])})'

        # Filling the table temp2
        for i in range(1, len(new_result)):
            self.connectionHelper.execute_sql(["INSERT INTO temp2" + str(t1) + " VALUES" + str(new_result[i]) + "; "])

        len1 = self.connectionHelper.execute_sql_fetchone_0("select count(*) from (select * from temp1 except all "
                                                            "select * from temp2) as T;")

        len2 = self.connectionHelper.execute_sql_fetchone_0("select count(*) from (select * from temp2 except all "
                                                            "select * from temp1) as T;")

        # Drop the temporary table and view created.
        self.connectionHelper.execute_sql(["drop view temp1;", "drop table temp2;"])

        if len1 == 0 and len2 == 0:
            return True
        else:
            return False

    def match1(self, Q_E, new_result):
        res, des = self.connectionHelper.execute_sql_fetchall(Q_E)
        new_result_ = new_result
        """
        for i in range(len(res)):
            flag = False
            temp = ()
            for ele in (res[i]):
                ele = str(ele)
                temp += (ele,)
            for j in range(len(new_result)):
                if temp == new_result_[j]:
                    new_result_[j] = []
                    flag = True
                    break
            if not flag:
                return False
        return True 
        """
        for row in res:
            row_str = tuple(str(val) for val in row)
            if row_str in new_result_:
                new_result_[new_result_.index(row_str)] = ()
            else:
                return False
        return True

    def nep_db_minimizer(self, query, tabname, Q_E, core_sizes, partition_dict, i):
        # Run the hidden query on this updated database instance with table T_u
        new_result = self.app.doJob(query)

        # Base Case
        if core_sizes == 1 and not self.match(Q_E, new_result):
            val = self.extractNEPValue(query, tabname, i)
            if val:
                return self.query_generator.updateExtractedQueryWithNEPVal(query, val)
            else:
                return Q_E

        # Drop the current view of name tabname
        # Make a view of name x with first half  T <- T_u
        self.connectionHelper.execute_sql(["drop view " + tabname + ";",
                                           self.create_view_with_upper_half(partition_dict, tabname)])

        if not self.match(Q_E, new_result):
            Q_E_ = self.nep_db_minimizer(query, tabname, Q_E, int(partition_dict[1] / 2),
                                         (int(partition_dict[0]), int(partition_dict[1] / 2)), i)
        else:
            Q_E_ = Q_E
        return self.get_QE_from_lower_half(Q_E_, i, partition_dict, query, tabname)

    def get_QE_from_lower_half(self, Q_E_, i, partition_dict, query, tabname):
        # Drop the view of name tabname
        # Make a view of name x with second half  T <- T_l
        self.connectionHelper.execute_sql(["drop view " + tabname + ";",
                                           self.create_view_with_lower_half(partition_dict, tabname)])
        # Run the hidden query on this updated database instance with table T_l
        new_result = self.app.doJob(query)
        if not self.match(Q_E_, new_result):
            Q_E__ = self.nep_db_minimizer(query, tabname, Q_E_, int(partition_dict[1]) - int(partition_dict[1] / 2),
                                          (int(partition_dict[0]) + int(partition_dict[1] / 2),
                                           int(partition_dict[1]) - int(partition_dict[1] / 2)), i)
            return Q_E__
        else:
            return Q_E_

    def create_view_with_lower_half(self, partition_dict, tabname):
        return "create view " + tabname + " as select * from " + get_restore_name(
            tabname) + " order by " + self.global_pk_dict[tabname] + " offset " + str(
            int(partition_dict[0]) + int(
                partition_dict[1] / 2)) + " limit " + str(
            int(partition_dict[1]) - int(partition_dict[1] / 2)) + ";"

    def create_view_with_upper_half(self, partition_dict, tabname):
        return "create view " + tabname + " as select * from " + get_restore_name(tabname) \
            + " order by " + self.global_pk_dict[tabname] + " offset " + str(
                int(partition_dict[0])) + " limit " + str(
                int(partition_dict[1] / 2)) + ";"

    def extractNEPValue(self, query, tabname, i):
        # Return if hidden executable is giving non-empty output on the reduced database
        # It means that the current table doesnot contain NEP source column
        new_result = self.app.doJob(query)
        if len(new_result) > 1:
            return False

        # check nep for every non-key attribute by changing its value to different s value and run the executable.
        # If the output came out non- empty. It means that nep is present on that attribute with previous value.
        attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}

        filter_attrib_dict = self.construct_filter_attribs_dict()

        attrib_list = self.global_all_attribs[i]
        filterAttribs = []

        # convert the view into a table
        self.connectionHelper.execute_sql(["alter view " + tabname + " rename to " + tabname + "_nep ;",
                                           "create table " + tabname + " as select * from " + tabname + "_nep ;"])

        for attrib in attrib_list:
            if attrib not in self.global_key_attributes:
                if 'date' in attrib_types_dict[(tabname, attrib)]:
                    if (tabname, attrib) in filter_attrib_dict.keys():
                        val = min(filter_attrib_dict[(tabname, attrib)][0],
                                  filter_attrib_dict[(tabname, attrib)][1])
                    else:
                        val = get_dummy_val_for('date')
                    val = ast.literal_eval(get_format(val))

                elif ('int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[
                    (tabname, attrib)]):
                    # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
                    if (tabname, attrib) in filter_attrib_dict.keys():
                        val = min(filter_attrib_dict[(tabname, attrib)][0],
                                  filter_attrib_dict[(tabname, attrib)][1])
                    else:
                        val = get_dummy_val_for('int')
                else:
                    if (tabname, attrib) in filter_attrib_dict.keys():
                        val = (filter_attrib_dict[(tabname, attrib)].replace('%', ''))
                    else:
                        val = get_char(get_dummy_val_for('char'))

                # prev = self.connectionHelper.execute_sql_fetchone_0("SELECT " + attrib + " FROM " + tabname + ";")

                if 'date' in attrib_types_dict[(tabname, attrib)]:
                    update_q = "UPDATE " + tabname + " SET " + attrib + " = " + val + ";"
                elif 'int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]:
                    update_q = "UPDATE " + tabname + " SET " + attrib + " = " + str(val) + ";"
                else:
                    update_q = "UPDATE " + tabname + " SET " + attrib + " = '" + val + "';"
                self.connectionHelper.execute_sql([update_q])

                new_result = self.app.doJob(query)
                # To make decimal precision

                # If it is non-empty
                if len(new_result) > 1:
                    # print(new_result, "yessssssss")
                    # if 'int' in attrib_types_dict[(tabname, attrib)] or \
                    #        'numeric' in attrib_types_dict[(tabname, attrib)]:
                    #    prev = "{0:.2f}".format(prev)
                    #    prev = decimal.Decimal(prev)

                    # convert the table back to view
                    self.connectionHelper.execute_sql(["drop table " + tabname + ";",
                                                       "alter view " + tabname + "_nep rename to " + tabname + ";"])
                    return filterAttribs

                    # convert the table back to view
        self.connectionHelper.execute_sql(["drop table " + tabname + ";",
                                           "alter view " + tabname + "_nep rename to " + tabname + ";"])
        return False
