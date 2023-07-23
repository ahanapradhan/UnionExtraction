import logging

import psycopg2


class ConnectionHelper:
    conn = None
    params = None

    def __init__(self, dbname, user, password, port, host="localhost"):
        self.params = "dbname=" + dbname + " user=" + user + " password=" + password + " host=" + host + " port=" + port

    def closeConnection(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def connectUsingParams(self):
        self.conn = psycopg2.connect(self.params)

    def getConnection(self):
        if self.conn is None:
            print("connecting...")
            self.connectUsingParams()
            print("done!")
        return self.conn

    def execute_sql(self, sqls):
        cur = self.conn.cursor()
        # print(cur)
        for sql in sqls:
            cur.execute(sql)
        cur.close()

    def execute_sql_fetchone(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        prev = cur.fetchone()
        prev = prev[0]
        cur.close()
        return prev

    def execute_sql_fetchall(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        des = cur.description
        cur.close()
        return res, des
