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

