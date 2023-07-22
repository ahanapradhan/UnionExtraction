import logging

import psycopg2


class ConnectionHelper:
    conn = None
    params = None

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ConnectionHelper, cls).__new__(cls)
        return cls.instance

    def __init__(self, dbname, user, password, port, host="localhost"):
        self.params = "dbname=" + dbname + " user=" + user + " password=" + password + " host=" + host + " port=" + port
        print(self.params)

    def __init__(self):
        self.params = None
        print("No connection params set")

    def closeConnection(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def connectToPostgreSQL(self, dbname, user, password, host="localhost"):
        try:
            self.params = "dbname=" + dbname + " user=" + user + " password=" + password + " host=" + host
            logging.debug(self.params)
            self.connectUsingParams()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def connectUsingParams(self):
        self.conn = psycopg2.connect(self.params)

    def getConnection(self):
        if self.conn is None:
            print("connecting...")
            self.connectUsingParams()
            print("done!")
        return self.conn
