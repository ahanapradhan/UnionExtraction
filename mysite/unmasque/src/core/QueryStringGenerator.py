import copy

from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import get_format, get_datatype, get_min_and_max_val


def refine_aggregates(agg, wc):
    for i, attrib in enumerate(agg.global_projected_attributes):
        if attrib in wc.global_key_attributes and attrib in agg.global_groupby_attributes:
            if not any(keyword in agg.global_aggregated_attributes[i][1] for keyword in ['Sum', 'Count']):
                agg.global_aggregated_attributes[i] = (agg.global_aggregated_attributes[i][0], '')
    temp_list = copy.deepcopy(agg.global_groupby_attributes)
    for attrib in temp_list:
        if attrib not in agg.global_projected_attributes:
            agg.global_groupby_attributes.remove(attrib)
        else:
            if not any(elt[0] == attrib and ('Sum' in elt[1] or 'Count' in elt[1]) for elt in
                       agg.global_aggregated_attributes):
                agg.global_groupby_attributes.remove(attrib)


def handle_range_preds(datatype, pred, pred_op):
    min_val, max_val = get_min_and_max_val(datatype)
    min_present = False
    max_present = False
    if pred[3] == min_val:  # no min val
        min_present = True
    if pred[4] == max_val:  # no max val
        max_present = True
    if min_present and not max_present:
        pred_op += " <= " + get_format(datatype, pred[4])
    elif not min_present and max_present:
        pred_op += " >= " + get_format(datatype, pred[3])
    elif not min_present and not max_present:
        pred_op += " >= " + get_format(datatype, pred[3]) + " and " + pred[1] + " <= " + get_format(
            datatype,
            pred[4])
    return pred_op


class QueryStringGenerator(Base):
    max_str_len = 500

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Query String Generator")
        self.app = Executable(connectionHelper)
        self.select_op = ''
        self.from_op = ''
        self.where_op = ''
        self.group_by_op = ''
        self.order_by_op = ''
        self.limit_op = None

    def generate_query_string(self, core_relations, wc, pj, gb, agg, ob, lm):
        self.from_op = ", ".join(core_relations)
        joins = []
        for edge in wc.global_join_graph:
            joins.append(" = ".join(edge))
        self.where_op = " and ".join(joins)

        if len(joins) > 0 and len(wc.filter_predicates) > 0:
            self.where_op += " and "
        self.where_op = self.add_filters(wc)

        eq = self.refine_Query(wc, pj, gb, agg, ob, lm)
        return eq

    def add_filters(self, wc):
        filters = []
        for pred in wc.filter_predicates:
            tab_col = tuple(pred[:2])
            pred_op = pred[1] + " "
            datatype = get_datatype(wc.global_attrib_types, tab_col)
            if pred[2] != ">=" and pred[2] != "<=" and pred[2] != "range":
                if pred[2] == "equal":
                    pred_op += " = "
                else:
                    pred_op += pred[2] + " "
                pred_op += get_format(datatype, pred[3])
            else:
                pred_op = handle_range_preds(datatype, pred, pred_op)

            filters.append(pred_op)
        self.where_op += " and ".join(filters)
        return self.where_op

    def assembleQuery(self):
        output = "Select " + self.select_op \
                 + "\n" + "From " + self.from_op
        if self.where_op != '':
            output = output + "\n" + "Where " + self.where_op
        if self.group_by_op != '':
            output = output + "\n" + "Group By " + self.group_by_op
        if self.order_by_op != '':
            output = output + "\n" + "Order By " + self.order_by_op
        if self.limit_op is not None:
            output = output + "\n" + "Limit " + self.limit_op
        output = output + ";"
        return output

    def refine_Query(self, wc, pj, gb, agg, ob, lm):
        refine_aggregates(agg, wc)

        # UPDATE OUTPUTS
        self.group_by_op = ", ".join(agg.global_groupby_attributes)

        for i, elt in enumerate(agg.global_projected_attributes):
            if agg.global_aggregated_attributes[i][1] != '':
                suffix = f'({elt})' if elt else ""
                elt = agg.global_aggregated_attributes[i][1] + suffix
                if 'Count' in agg.global_aggregated_attributes[i][1]:
                    elt = agg.global_aggregated_attributes[i][1]
            if elt and elt != pj.projection_names[i]:
                elt = f'{elt} as {pj.projection_names[i]}' if pj.projection_names[i] else elt
            self.select_op += ", " + elt if self.select_op else elt

        self.order_by_op = ob.orderBy_string[:-2]
        if lm.limit is not None:
            self.limit_op = str(lm.limit)
        eq = self.assembleQuery()
        return eq

    def updateExtractedQueryWithNEPVal(self, query, val):
        for elt in val:
            if '-' in str(elt[3]):
                predicate = elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "

            elif isinstance(elt[3], str):
                output = self.getStrFilterValue(query, elt[0], elt[1], elt[3], self.max_str_len)
                if '%' in output or '_' in output:
                    predicate = elt[1] + " NOT LIKE '" + str(output) + "' "
                else:
                    predicate = elt[1] + " " + str(elt[2]) + " '" + str(output) + "' "
            else:
                predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[3])

            self.where_op = f'{self.where_op} and {predicate}' if self.where_op else predicate

        Q_E = self.assembleQuery()
        return Q_E

    def getStrFilterValue(self, query, tabname, attrib, representative, max_length):
        # convert the view into a table
        self.connectionHelper.execute_sql(["alter view " + tabname + " rename to " + tabname + "_nep ;",
                                           "create table " + tabname + " as select * from " + tabname + "_nep ;"])

        index = 0
        output = ""
        # currently inverted exclaimaination is being used assuming it will not be in the string
        # GET minimal string with _
        while index < len(representative):
            temp = list(representative)
            if temp[index] == 'a':
                temp[index] = 'b'
            else:
                temp[index] = 'a'
            temp = ''.join(temp)

            # updatequery
            u_query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
            self.connectionHelper.execute_sql([u_query])
            new_result = self.app.doJob(query)
            if len(new_result) <= 1:
                temp = copy.deepcopy(representative)
                temp = temp[:index] + temp[index + 1:]

                # updatequery
                u_query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
                self.connectionHelper.execute_sql([u_query])
                new_result = self.app.doJob(query)
                if len(new_result) <= 1:
                    representative = representative[:index] + representative[index + 1:]
                else:
                    output = output + "_"
                    representative = list(representative)
                    representative[index] = u"\u00A1"
                    representative = ''.join(representative)
                    index = index + 1
            else:
                output = output + representative[index]
                index = index + 1
        if output == '':
            # convert the table back to view
            self.connectionHelper.execute_sql(["drop table " + tabname + ";",
                                               "alter view " + tabname + "_nep rename to " + tabname + ";"])
            return output
        # GET % positions
        index = 0
        representative = copy.deepcopy(output)
        if len(representative) < max_length:
            output = ""
            while index < len(representative):
                temp = list(representative)
                if temp[index] == 'a':
                    temp.insert(index, 'b')
                else:
                    temp.insert(index, 'a')
                temp = ''.join(temp)
                # updatequery
                u_query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
                self.connectionHelper.execute_sql([u_query])
                new_result = self.app.doJob(query)
                # update_other_data(tabname, attrib, 'text', temp, new_result, [])
                if len(new_result) <= 1:
                    output = output + '%'
                output = output + representative[index]
                index = index + 1
            temp = list(representative)
            if temp[index - 1] == 'a':
                temp.append('b')
            else:
                temp.append('a')
            temp = ''.join(temp)

            # updatequery
            u_query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
            self.connectionHelper.execute_sql([u_query])
            new_result = self.app.doJob(query)
            if len(new_result) <= 1:
                output = output + '%'

        # convert the table back to view
        self.connectionHelper.execute_sql(["drop table " + tabname + ";",
                                           "alter view " + tabname + "_nep rename to " + tabname + ";"])
        return output
