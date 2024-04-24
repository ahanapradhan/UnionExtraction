from ...src.core.executable import Executable


def is_result_nonempty_nullfree(res, logger=None):
    if logger is not None:
        logger.debug(res[1:])
    if res[1:] is None:
        return False
    if res[1:] == [None]:
        return False
    if not len(res[1:]):
        return False
    for row in res[1:]:
        if any(val in [None, 'None'] for val in row):
            return False
    return True


class NullFreeExecutable(Executable):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper)
        self.extractor_name = "Null Free Executable"

    """
    def doActualJob(self, args=None):
        query = self.extract_params_from_args(args)
        result = False
        try:
            res, description = self.connectionHelper.execute_sql_fetchall(query, self.logger)
            if res is not None:
                result = is_result_nullfree(res)

        except Exception as error:
            self.logger.error("Executable could not be run. Error: " + str(error))
            raise error

        return result
    """

    def isQ_result_empty(self, Res):
        return not is_result_nonempty_nullfree(Res, self.logger)
