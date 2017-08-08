from tools import *
#from fetchinstrumentoanda import *


class AssetInfo:

    def __init__(self, name, database_info, granularity_list):
        assert len(granularity_list) > 0
        assert self.checkGranularity(granularity_list)
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

    def checkGranularity(self, gran_list):
        keysList = Utility.granularityToSeconds
        results = True
        for gran in gran_list:
            results = results and (gran in keysList)
        return results

    def hasBeenInit(self):
        return(self.lastUpdateTime == 0)

    def setUpdateTime(self):
        self.lastUpdateTime = Utility.getLondonUNIXDate()
        print("Set new update date: {}".format(self.lastUpdateTime))
