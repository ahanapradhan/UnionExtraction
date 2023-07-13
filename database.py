from db_actions import DbMock, DbParser


class Schema:
    ddot = []
    db = DbMock()
    parser = DbParser()

    def get_relations(self):
        raise NotImplementedError("method not implemented")

    def nullify_except(self, s_set):
        raise NotImplementedError("method not implemented")

    def run_query(self, QH):
        raise NotImplementedError("method not implemented")

    def get_partial_QH(self, QH):
        self.parser.parse(QH)
        return self.parser.parttab_QH


class TPCH(Schema):
    relations = ["orders", "lineitem", "customer", "supplier", "part", "partsupp", "nation", "region"]

    def get_relations(self):
        return self.relations

    def nullify_except(self, s_set):
        self.ddot = self.db.nullify_except(set(self.relations), s_set)
        return self.ddot

    def run_query(self, QH):
        self.parser.parse(QH)
        froms = set(self.parser.fromtab_QH)
        is_subset = froms.issubset(self.ddot)
        return is_subset


