import psycopg2
from fetchinstrumentoanda import *
from tools import *
from databaseinfo import *
from assetinfo import *

class Worker:
    # depth of the initialization of assets
    INITIAL_ASSET_DATA_DEPTH = 50

    def __init__(self, name):
        assert isinstance(name, str)
        self.name = name
        # Dict Database Name => Database Info
        self.databaseConnectionsDict = {}
        # Dict Database => Dict (Asset => Asset info)
        self.assetsPerDatabaseDict = {}

    def __str__(self):
        assert(len(self.databaseConnectionsDict.keys()) == len(self.assetsPerDatabaseDict.keys()))
        reprst = "Hi! I am " + self.name + "!\n"
        reprst += "Database(s): \n"
        for database in self.databaseConnectionsDict.keys():
            reprst += "\t" + str(database) + "\n"
            for asset in self.assetsPerDatabaseDict[database].values():
                reprst += asset.__str__()

        return reprst

    def addDatabase(self, database_info):
        if not isinstance(database_info, DatabaseInfo):
            raise Exception('database_info: wrong input')
        else:
            self.databaseConnectionsDict[database_info.getName()] = database_info

    def addAssetToDatabase(self, database_str, asset_info):
        assert isinstance(asset_info, AssetInfo)
        assert isinstance(database_str, str)

        if not database_str in self.assetsPerDatabaseDict.keys():
            self.assetsPerDatabaseDict[database_str] = {}

        self.assetsPerDatabaseDict[database_str][asset_info.getName()] = asset_info
        print(self.assetsPerDatabaseDict[database_str][asset_info.getName()])

    def addGranularitytoAsset(self, database_str, asset_str, granularity_list):
        temp = self.assetsPerDatabaseDict[database_str][asset_str]
        if temp is None:
            raise Exception(asset_str + ' is not in database ' + database_str)
        else:
            temp.addGranularity(granularity_list)

    def update(self):
        # initialise data of given assets
        for database in self.databaseConnectionsDict.keys():
            for asset in self.assetsPerDatabaseDict[database]:
                print(ok)
