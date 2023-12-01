from mysite.unmasque.src.core.abstract.spj_QueryStringGenerator import SPJQueryStringGenerator


class IntersectionQgenerator(SPJQueryStringGenerator):
    def __init__(self, connHelper):
        super().__init__(connHelper)

    def generate_setOp_query_String(self, subquery_data):
        subq_strings = []
        for i in range(len(subquery_data)):
            subquery = subquery_data[i]
            subq_modules = [subquery.equi_join, subquery.filter,
                            subquery.projection, subquery.from_clause.core_relations]
            subq_str = "(" + self.generate_query_string(subq_modules) + ")"
            subq_str = subq_str.replace(";", "")
            subq_strings.append(subq_str)
        eq = "\n INTERSECT \n".join(subq_strings)
        eq = eq + ";"
        return eq
