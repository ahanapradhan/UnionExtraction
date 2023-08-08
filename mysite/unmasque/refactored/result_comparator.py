from .abstract.ExtractorBase import Base


class ResultComparator(Base):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Result Comparator")

    def extract_params_from_args(self, args):
        return args[0], args[1]

    def doActualJob(self, args):
        Q_h, Q_E = self.extract_params_from_args(args)
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

        if len(res) == 1 and len(res[0]) == 1:
            if res is not None:
                for row in res:
                    if any(val is not None for val in row):
                        ins = tuple(str(val) for val in row)
                        self.connectionHelper.execute_sql(
                            ['INSERT INTO r_h' + str(t1) + ' VALUES (' + str(ins[0]) + '); '])

        else:
            if res is not None:
                for row in res:
                    if all(val is None for val in row):
                        continue
                    ins = tuple(str(val) for val in row)
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
