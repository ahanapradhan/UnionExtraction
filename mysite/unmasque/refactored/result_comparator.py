from .abstract.ExtractorBase import Base
from .util.common_queries import get_restore_name, drop_table, alter_table_rename_to
from ..src.pipeline.abstract.TpchSanitizer import TpchSanitizer


class ResultComparator(Base, TpchSanitizer):

    def __init__(self, connectionHelper, isHash):
        super().__init__(connectionHelper, "Result Comparator")
        self.isHash = isHash

    def extract_params_from_args(self, args):
        return args[0], args[1]

    def match_hash_based(self, Q_h, Q_E):
        self.connectionHelper.execute_sql(["create view r_e as " + Q_E])

        res1 = self.connectionHelper.execute_sql_fetchone_0("Select count(*) from r_e")

        res, des = self.connectionHelper.execute_sql_fetchall(
            Q_h)  # fetchone always return a tuple whereas fetchall return list

        colnames = [desc[0] for desc in des]

        if res1 != (len(res)):
            return False

        result = [tuple(colnames)]
        # print(result) it will print 8 times for from clause
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
        if len1 == len2:
            return True
        else:
            return False

    def doActualJob(self, args):
        Q_h, Q_E = self.extract_params_from_args(args)
        self.sanitize()
        if self.isHash:
            matched = self.match_hash_based(Q_h, Q_E)
        else:
            matched = self.match_comparison_based(Q_h, Q_E)
        return matched

    def match_comparison_based(self, Q_h, Q_E):
        # Run the extracted query Q_E .
        self.connectionHelper.execute_sql(['create view temp1 as ' + Q_E])

        # Size of the table
        res = self.connectionHelper.execute_sql_fetchone_0('select count(*) from temp1;')

        # Create an empty table with name temp2
        self.connectionHelper.execute_sql(['Create table temp2 (like temp1);'])

        res, desc = self.connectionHelper.execute_sql_fetchall(Q_h)
        colnames = [d[0] for d in desc]

        new_result = [tuple(colnames)]

        # Header of temp2
        t = new_result[0]
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
                        ['INSERT INTO temp2' + str(t1) + ' VALUES (' + str(ins[0]) + '); '])
                else:
                    self.connectionHelper.execute_sql(['INSERT INTO temp2' + str(t1) + ' VALUES' + str(ins) + '; '])

        len1 = self.connectionHelper.execute_sql_fetchone_0(
            'select count(*) from (select * from temp1 except all select * from temp2) as T;')
        len2 = self.connectionHelper.execute_sql_fetchone_0(
            'select count(*) from (select * from temp2 except all select * from temp1) as T;')

        self.connectionHelper.execute_sql(["drop view temp1;", "drop table temp2;"])

        if not len1 and not len2:
            return True
        else:
            return False
