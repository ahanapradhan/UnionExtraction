from ...src.core.executable import Executable


def log_result(Res, logger):
    logger.debug("Result size: ", len(Res))
    if len(Res) < 10:
        for row in Res:
            logger.debug(row)


def is_result_nonempty_nullfree(res, logger=None):
    if logger is not None:
        logger.debug("is_result_nonempty_nullfree")
        log_result(res, logger)
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
        logger.debug("is_result_all_null")
        log_result(res, logger)

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
        logger.debug("is_result_has_no_data")
        log_result(res, logger)

    if len(res) <= 1:
        return True
    return False


def is_result_has_some_data(res, logger=None):
    if logger is not None:
        logger.debug("is_result_has_some_data")
        log_result(res, logger)

    if res[1:] is None:
        return False
    if res[1:] == [None]:
        return False
    if not len(res[1:]):
        return False
    for row in res[1:]:
        if any(val not in [None, 'None'] for val in row):
            return True
    return False


def is_result_no_full_nullfree_row(res, logger):
    if logger is not None:
        logger.debug("is_result_no_full_nullfree_row")
        log_result(res, logger)

    if res[1:] is None:
        return True
    if res[1:] == [None]:
        return True
    if not len(res[1:]):
        return True
    for row in res[1:]:
        if all(val not in [None, 'None'] for val in row):
            return False
    return True


class NullFreeExecutable(Executable):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Null Free Executable")

    def is_attrib_all_null(self, Res, attrib):
        idx = Res[0].index(attrib)
        yes = True
        for row in Res[1:]:
            self.logger.debug(f"{attrib} value: ", row[idx])
            if row[idx] in [None, 'None']:
                yes = yes and True
            else:
                yes = False
                return yes
        return yes

    def isQ_result_no_full_nullfree_row(self, Res):
        return is_result_no_full_nullfree_row(Res, self.logger)

    def isQ_result_nonEmpty_nullfree(self, Res):
        return is_result_nonempty_nullfree(Res, self.logger)

    def isQ_result_empty(self, Res):
        return not is_result_nonempty_nullfree(Res, self.logger)

    def isQ_result_has_no_data(self, Res):
        return is_result_has_no_data(Res, self.logger)

    def isQ_result_all_null(self, Res):
        return is_result_all_null(Res, self.logger)

    def isQ_result_has_some_data(self, Res):
        return is_result_has_some_data(Res, self.logger)
