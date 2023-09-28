from psycopg2 import Error

from .util.common_queries import drop_view, get_row_count
from ..src.pipeline.abstract.Comparator import Comparator


class ResultComparator(Comparator):

    def __init__(self, connectionHelper, isHash):
        super().__init__(connectionHelper, "Result Comparator", isHash)

    def match_hash_based(self, Q_h, Q_E):
        self.connectionHelper.execute_sql([drop_view("r_e"),
                                           "create view r_e as " + Q_E])

        res1 = self.connectionHelper.execute_sql_fetchone_0(get_row_count("r_e"))

        res, des = self.connectionHelper.execute_sql_fetchall(
            Q_h)  # fetchone always return a tuple whereas fetchall return list

        colnames = [desc[0] for desc in des]

        if res1 != (len(res)):
            return False

        result = [tuple(colnames)]
        self.logger.debug(result)  # it will print 8 times for from clause
        # it will append attribute name in the result

        self.connectionHelper.execute_sql(['Create unlogged table r_h (like r_e);'])

        # Header of r_h
        t = result[0]
        t1 = '(' + ', '.join(t) + ')'

        if res is not None:
            for row in res:
                # CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
                nullrow = True
                for val in row:
                    if val is not None:
                        nullrow = False
                        break
                if nullrow:
                    continue
                temp = []
                for val in row:
                    temp.append(str(val))
                ins = (tuple(temp))
                if len(res) == 1 and len(res[0]) == 1:
                    self.connectionHelper.execute_sql(
                        ['INSERT INTO r_h' + str(t1) + ' VALUES (' + str(ins[0]) + '); '])
                else:
                    self.connectionHelper.execute_sql(['INSERT INTO r_h' + str(t1) + ' VALUES' + str(ins) + '; '])

        len1 = self.connectionHelper.execute_sql_fetchone_0(
            "select sum(hashtext) from (select hashtext(r_e::TEXT) FROM r_e) "
            "as T;")

        len2 = self.connectionHelper.execute_sql_fetchone_0("select sum(hashtext) from (select hashtext(r_h::TEXT) "
                                                            "FROM r_h) as T;")

        self.connectionHelper.execute_sql(['DROP view r_e;', 'DROP TABLE r_h;'])
        self.logger.debug(len1, len2)
        if len1 == len2:
            return True
        else:
            return False

    def insert_data_into_Qh_table(self, res_Qh):
        header = res_Qh[0]
        res_Qh_ = res_Qh[1:]
        if res_Qh_ is not None:
            for row in res_Qh_:
                # CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
                nullrow = True
                for val in row:
                    if val is not None:
                        nullrow = False
                        break
                if nullrow:
                    continue
                temp = []
                for val in row:
                    temp.append(str(val))
                ins = (tuple(temp))
                if len(res_Qh_) == 1 and len(res_Qh_[0]) == 1:
                    self.insert_into_temp2_values(header, ins[0])
                else:
                    self.insert_into_temp2_values(header, ins)


