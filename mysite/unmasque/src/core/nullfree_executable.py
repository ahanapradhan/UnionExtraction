from ...src.core.executable import Executable, is_error


def is_result_nullfree(res):
    for row in res:
        # Check if the whole row is None (SPJA Case)
        if any(val is None for val in row):
            return False
    return True


class NullFreeExecutable(Executable):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper)

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
