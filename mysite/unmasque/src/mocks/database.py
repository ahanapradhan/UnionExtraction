from .db_actions import DbMock, DbParser


class Schema:
    comtabs = set()
    fromtabs = set()

    def get_relations(self):
        raise NotImplementedError("method not implemented")

    def nullify_except(self, s_set):
        raise NotImplementedError("method not implemented")

    def run_query(self, QH):
        raise NotImplementedError("method not implemented")

    def get_partial_QH(self, QH):
        raise NotImplementedError("method not implemented")

    def revert_nullify(self):
        pass

    def isEmpty(self, Res):
        if not Res:
            return True
        return False


class TPCH(Schema):
    ddot = []
    db = DbMock()
    parser = DbParser()
    relations = ["orders", "lineitem", "customer", "supplier", "part", "partsupp", "nation", "region"]

    def get_relations(self):
        return self.relations

    def nullify_except(self, s_set):
        self.ddot = self.db.nullify_except(set(self.relations), s_set)
        return self.ddot

    def run_query(self, QH):
        self.parser.parse(QH)
        for form in self.parser.fromtabs_qi:
            if form.issubset(self.ddot):
                return True
        return False

    def get_partial_QH(self, QH):
        self.parser.parse(QH)
        return self.parser.parttab_QH

