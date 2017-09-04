from tools import Utility
import oandapyV20 as oandapy


class DatabaseInfo:
    def __init__(self, name, conn):
        self.name = name
        self.conn = conn
        self.tool = None
        if name is 'oanda':
            self.tool = oandapy.API(environment="practice",
                                    access_token=Utility.getAccountToken(),
                                    headers={'Accept-Datetime-Format': 'UNIX'})

    def __str__(self):
        return 'Database: ' + self.name + '\n'

    def getName(self):
        return self.name

    def getConnection(self):
        return self.conn

    def getTool(self):
        return self.tool

    # To be keys in dict:
    def __hash__(self):
        return hash((self.name, self.conn))

    def __eq__(self, other):
        return (self.name, self.conn) == (other.name, other.conn)
