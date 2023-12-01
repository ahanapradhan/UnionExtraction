from mysite.unmasque.src.core.IntersectionQgenetaor import IntersectionQgenerator
from mysite.unmasque.src.core.UnionQgenerator import UnionQgenerator


class QueryGeneratorFactory:
    def __init__(self, is_intersection, connectionHelper):
        if is_intersection:
            self.q_generator = IntersectionQgenerator(connectionHelper)
        else:
            self.q_generator = UnionQgenerator(connectionHelper)

    def generate_setOp_query_string(self, subquery_data):
        return self.q_generator.generate_setOp_query_String(subquery_data)

    def generate_query_string(self, pipeline_modules):
        return self.q_generator.generate_query_string(pipeline_modules)
