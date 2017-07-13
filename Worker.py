import psycopg2
from FetchOANDA import *
from Utility import *
class Worker:

    def __init__(self, name):
        assert isinstance(name, str)
        self.name = name
        # Dict Database Name => Database Info
        self.databaseConnectionsDict = {}
        # Dict Database => Dict (Asset => Asset info)
        self.assetsPerDatabaseDict = {}

    def __str__(self):
        assert len(self.databaseConnectionsDict.keys()) == len(self.assetsPerDatabaseDict.keys())
        reprst = "Hi! I am " + self.name + "!\n"
        reprst += "Database(s): \n"
        for database in self.databaseConnectionsDict.keys():
            reprst += str(database) + "\n"
            for asset in self.assetsPerDatabaseDict[database].values():
                reprst += asset.__str__()

        return reprst

    def addDatabase(self, database_info):
        if not isinstance(database_info, DatabaseInfo):
            raise Exception('database_info: wrong input')
        else:
            self.databaseConnectionsDict[database_info.getName()] = database_info

    def addAssetOnDatabase(self, database_str, asset_info):
        assert isinstance(asset_info, AssetInfo)

        if self.assetsPerDatabaseDict[database_str] is None:
            self.assetsPerDatabaseDict[database_str] = {}

        self.assetsPerDatabaseDict[database_str][asset_info.getName()] = asset_info

    def addGranularitytoAsset(self, database_str, asset_str, granularity_list):
        temp = self.assetsPerDatabaseDict[database_str][asset_str]
        if temp is None:
            raise Exception(asset_str + ' is not in database ' + database_str)
        else:
            temp.granularity.append(granularity_list)

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


class AssetInfo:

    def __init__(self, name, database_info, granularity_list):
        self.name = name
        self.lastUpdateTime = 0;
        self.database_info = database_info
        if isinstance(granularity_list, str):
            self.tools = Utility.getAssetUpdateTool(self.name, database_info, granularity_list)
        else:
            self.tools = Utility.getAssetUpdateToolDict(self.name, database_info, granularity_list)
        # dict Keys = Gran, Values = Update tool

    def __str__(self):
        reprst =  'Asset Information: \n' + '\t' + 'Name: ' + self.name + '\n'
        reprst += '\t' + 'Database: ' + self.database_info.getName() + '\n'
        reprst += '\t' + 'Granularity: '
        for gran in self.tools.keys():
            reprst += gran + ' '
        reprst += '\n'
        return reprst

    def getName(self):
        return self.name

    def getGranularity(self):
        return self.tools.keys()

    def addGranularity(self, gran_list):
        assert isinstance(gran_list, list)
        self.tools = {**self.tools, **Utility.getAssetUpdateToolDict(self.name, self.database_info, gran_list)}
