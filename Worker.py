import psycopg2

class Worker:

    def __init__(self):
        self.databaseConnectionsDict = {}
        self.assetsPerDatabaseDict = {}

    def __str__(self):
        return "My name is Dave! Working on:\n" +
         self.databaseConnectionsDict + "\n" +
         self.assetsPerDatabaseDict

    def addDatabase(self, database_str, conn):
        if not isinstance(database_str, str) :
            raise Exception('database_str ')
