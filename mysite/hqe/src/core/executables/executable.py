import time

from ..abstract.ExtractorBase import Base
from ...util.constants import REL_ERROR, RELATION, ORPHAN_COLUMN


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


def add_header(description, result):
    if description is not None and not is_error(description):
        colnames = [desc[0] for desc in description]
        for i in range(len(colnames)):
            if colnames[i] == '?column?':  # Changing the name of column to legitimate name.
                colnames[i] = ORPHAN_COLUMN
        result.append(tuple(colnames))


class Executable(Base):

    def __init__(self, connectionHelper, name="Executable"):
        super().__init__(connectionHelper, name)

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args=None):
        query = self.extract_params_from_args(args)
        result = []
        try:
            start_t = time.time()
            res, description = self.connectionHelper.execute_sql_fetchall(query, self.logger)
            end_t = time.time()
            self.logger.debug(f"Exe time: {str(end_t - start_t)}")
            add_header(description, result)
            if res is not None:
                result = get_result_as_tuple_1(res, result)
        except Exception as error:
            self.logger.error("Executable could not be run. Error: " + str(error))
            raise error
        return result

    def get_nullfree_row(self, Res):
        return Res[1]

    def get_all_nullfree_rows(self, Res):
        return Res[1:]

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
        return False

    def isQ_result_has_no_data(self, Res):
        return self.isQ_result_empty(Res)

    def isQ_result_all_null(self, Res):
        return self.isQ_result_empty(Res)

    def isQ_result_has_some_data(self, Res):
        return not self.isQ_result_empty(Res)
