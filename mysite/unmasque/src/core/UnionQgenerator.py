from mysite.unmasque.src.core.QueryStringGenerator import QueryStringGenerator


class UnionQgenerator(QueryStringGenerator):
    def __init__(self, connHelper):
        super().__init__(connHelper)

    def generate_setOp_query_String(self, subqueries):
        u_qs = []
        for subquery in subqueries:
            subquery = subquery.replace('Select', '(Select')
            subquery = subquery.replace(';', ')')
            u_qs.append(subquery)

        u_Q = "\n UNION ALL \n".join(u_qs)
        u_Q += ";"

        if "UNION ALL" not in u_Q:
            if u_Q.startswith('(') and u_Q.endswith(');'):
                u_Q = u_Q[1:-2] + ';'

        return u_Q
