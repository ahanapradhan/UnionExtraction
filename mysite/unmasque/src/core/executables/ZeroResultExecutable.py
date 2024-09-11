from ....src.core.executables.executable import Executable


class ZeroResultExecutable(Executable):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Zero_Result_Executable")

    def is_attrib_all_null(self, Res, attrib):
        return self.isQ_result_empty(Res)

    def get_attrib_val(self, Res, attrib_idx):
        return Res[1][attrib_idx]

    def isQ_result_no_full_nullfree_row(self, Res):
        return self.isQ_result_empty(Res)

    def isQ_result_nonEmpty_nullfree(self, Res):
        return not self.isQ_result_empty(Res)

    def isQ_result_empty(self, Res):
        # self.logger.debug("exe: isQ_result_empty")
        if len(Res) <= 1:
            return True
        if len(Res) >= 2:
            data = Res[1:]
            for row in data:
                check = all(val in [0, '0', None, 'None'] for val in row)
                if not check:
                    return False
        return True

    def isQ_result_has_no_data(self, Res):
        return self.isQ_result_empty(Res)

    def isQ_result_all_null(self, Res):
        return self.isQ_result_empty(Res)

    def isQ_result_has_some_data(self, Res):
        return not self.isQ_result_empty(Res)
