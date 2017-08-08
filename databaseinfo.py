from tools import *
import oandapyV20 as oandapy

class DatabaseInfo:
    def __init__(self, name, conn):
        self.name = name
        self.conn = conn
        if name is 'oanda':
            self.tool = oandapy.API(environment = "practice",
            access_token = Utility.getAccountToken(),
            headers={'Accept-Datetime-Format': 'UNIX'})

    def __str__(self):
        return 'Database: ' + self.name + '\n'

    def getName(self):
        return self.name

    def getConnection(self):
        return self.conn

    def getTool(self):
        return self.tool
