from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import get_datatype, get_format, get_min_and_max_val


def generate_join_string(edges):
    joins = []
    for edge in edges:
        if type(edge) is list:
            edge.sort()
        for i in range(len(edge) - 1):
            left_e = edge[i]
            right_e = edge[i + 1]
            join_e = f"{left_e} = {right_e}"
            joins.append(join_e)
    where_op = " and ".join(joins)
    return where_op


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


def get_modules(modules):
    core_relations, ej, fl, pj = modules[0], modules[1], modules[2], modules[3]
    return core_relations, ej, fl, pj


class SPJQueryStringGenerator(Base):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Query String Generator")
        self.app = Executable(connectionHelper)
        self.select_op = ''
        self.from_op = ''
        self.where_op = ''

    def refine_Query1(self, modules):
        pass

    def generate_query_string(self, modules):
        core_relations, ej, fl, pj = get_modules(modules)
        core_relations.sort()
        self.from_op = ", ".join(core_relations)
        self.where_op = generate_join_string(ej.global_join_graph)

        if fl is not None:
            if self.where_op and len(fl.filter_predicates) > 0:
                self.where_op += " and "
            self.where_op = self.add_filters(fl)

        self.refine_Query1(modules)
        eq = self.assembleQuery()
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
        output = output + ";"
        return output
