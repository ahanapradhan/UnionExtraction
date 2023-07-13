import re


class DbBase:
    def nullify_except(self, D, s_set):
        raise NotImplementedError("method not implemented")

    def run_query(self, Ddot, QH):
        raise NotImplementedError("method not implemented")


class DbMock(DbBase):
    mock = "mock"

    def nullify_except(self, D, s_set):
        print("DbMock...get_Ddot")
        print(D)
        print(s_set)
        diff = D.difference(s_set)
        print(diff)
        return diff

    def run_query(self, Ddot, QH):
        print("DbMock...run_query")
        print(QH)
        print(Ddot)
        return self.mock


class DbParser:
    fromtab_QH = set()
    comtab_QH = set()
    parttab_QH = set()
    fromtabs_qi = []
    parttabs_qi = []

    def parse(self, sql):
        subqueries = re.findall(r'\((.*?)\)', sql)
        for subquery in subqueries:
            tables = re.findall(r'FROM\s+(.*?)(?:\s+WHERE|\))', subquery, re.IGNORECASE)
            self.fromtabs_qi.append(set(tables))

        # Find tables present in all subqueries
        self.comtab_QH = set.intersection(*self.fromtabs_qi)

        for ft in self.fromtabs_qi:
            self.parttabs_qi.append(ft - self.comtab_QH)

        self.fromtab_QH = set().union(*self.fromtabs_qi)
        self.parttab_QH = set().union(*self.parttabs_qi)


    def print(self):
        print("fromtab_QH")
        print(self.fromtab_QH)
        print("comtab_QH")
        print(self.comtab_QH)
        print("parttab_QH")
        print(self.parttab_QH)
        print("fromtabs_qi")
        print(self.fromtabs_qi)
        print("parttabs_qi")
        print(self.parttabs_qi)



