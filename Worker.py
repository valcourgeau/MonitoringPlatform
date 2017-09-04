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

    def addAssetToDatabase(self, database_info, asset_info):
        assert isinstance(asset_info, AssetInfo)
        assert isinstance(database_info, DatabaseInfo)

        if not database_info in self.assetsPerDatabaseDict.keys():
            self.assetsPerDatabaseDict[database_info] = {}

        self.assetsPerDatabaseDict[database_info][asset_info] = asset_info
        print(self.assetsPerDatabaseDict[database_info][asset_info])

    def addGranularitytoAsset(self, database_info, asset_info, granularity_list):
        assert isinstance(asset_info, AssetInfo)
        assert isinstance(database_info, DatabaseInfo)

        temp = self.assetsPerDatabaseDict[database_info][asset_info]
        if temp is None:
            raise Exception(asset_info.getName() + ' is not in database ' + database_info.getName())
        else:
            assert isinstance(temp, AssetInfo)
            print("Adding gran...")
            temp.addGranularity(granularity_list)
            print("Successful!")

    def update(self):
        # initialise data of given assets
        for database in self.databaseConnectionsDict.keys():
            for asset in self.assetsPerDatabaseDict[database]:
                print("Update at {}".format(datetime.utcnow()))
