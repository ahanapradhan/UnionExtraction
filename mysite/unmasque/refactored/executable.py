from ..refactored.abstract.ExtractorBase import Base
from ..src.util.constants import REL_ERROR, RELATION


def get_result_as_tuple_1(res, result):
    for row in res:
        # Check if the whole row is None (SPJA Case)
        if all(val is None for val in row):
            continue

        # Convert all values in the row to strings and create a tuple
        row_as_tuple = tuple(str(val) for val in row)
        result.append(row_as_tuple)
    return result


def is_error(msg):
    if not isinstance(msg, str):
        return False
    l_msg = msg.lower()
    return RELATION.lower() in l_msg and REL_ERROR.lower() in l_msg


class Executable(Base):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Executable")

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        result = []
        try:
            res, description = self.connectionHelper.execute_sql_fetchall(query, self.logger)
            if description is not None and not is_error(description):
                colnames = [desc[0] for desc in description]
                result.append(tuple(colnames))
            if res is not None:
                result = get_result_as_tuple_1(res, result)

        except Exception as error:
            self.logger.error("Executable could not be run. Error: " + str(error))
            raise error

        return result
