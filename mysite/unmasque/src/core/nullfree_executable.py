from ...src.core.executable import Executable


def is_result_nonempty_nullfree(res, logger=None):
    if logger is not None:
        pass
        # logger.debug(res[1:])
    if res[1:] is None:
        return False
    if res[1:] == [None]:
        return False
    if not len(res[1:]):
        return False
    for row in res[1:]:
        if all(val not in [None, 'None'] for val in row):
            return True
    return False


def is_result_all_null(res, logger=None):
    if logger is not None:
        pass
    if res[1:] is None:
        return False
    if res[1:] == [None]:
        return True
    if not len(res[1:]):
        return False
    for row in res[1:]:
        if all(val in [None, 'None'] for val in row):
            return True
    return False


def is_result_has_no_data(res, logger=None):
    if logger is not None:
        pass
    if len(res) <= 1:
        return True
    return False


def is_result_has_some_null_data(res, logger=None):
    if logger is not None:
        pass
    return not is_result_has_no_data(res, logger) \
        and not is_result_all_null(res, logger) \
        and not is_result_nonempty_nullfree(res, logger)


class NullFreeExecutable(Executable):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper)
        self.extractor_name = "Null Free Executable"

    def isQ_result_nonEmpty_nullfree(self, Res):
        return is_result_nonempty_nullfree(Res, self.logger)

    def isQ_result_empty(self, Res):
        return not is_result_nonempty_nullfree(Res)

    def isQ_result_has_no_data(self, Res):
        return is_result_has_no_data(Res, self.logger)

    def isQ_result_all_null(self, Res):
        return is_result_all_null(Res, self.logger)

    def isQ_result_has_some_data(self, Res):
        return is_result_has_some_null_data(Res, self.logger)


