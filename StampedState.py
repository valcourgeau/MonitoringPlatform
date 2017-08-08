from datetime import datetime

class StampedState:
    # This class wraps the Mid/Mid/Ask CHOL prices obtain at a certain timestamp

    __priceKeys = {'c', 'h', 'o', 'l'}
    __ask = "ask"
    __bid = "bid"
    __mid = "mid"
    __volume = "volume"

    def rowNames():
        descStr = "InstrumentName,"
        descStr += "Timestamp,"
        descStr += "UTCdate,"
        descStr += "Volume,"
        descStr += "Ask.c,"
        descStr += "Ask.h,"
        descStr += "Ask.o,"
        descStr += "Ask.l,"
        descStr += "Bid.c,"
        descStr += "Bid.h,"
        descStr += "Bid.o,"
        descStr += "Bid.l,"
        descStr += "Mid.c,"
        descStr += "Mid.h,"
        descStr += "Mid.o,"
        descStr += "Mid.l,"
        return(descStr)

    def __init__(self, timestamp, databaseName, instrumentName, granularity):
        # timestamp in UNIX time
        self.databaseName = databaseName
        self.instrumentName = instrumentName
        self.timestamp = timestamp
        self.granularity = granularity
        self.UTCdate = datetime.utcfromtimestamp(self.timestamp)
        self.volume = 0
        self.prices = ({}, {}, {}) # Ask, Bid, Mid

    def __str__(self):
        descStr = str(self.databaseName) + ","
        descStr += str(self.instrumentName) + ","
        descStr += str(self.granularity) + ","
        descStr += str(self.timestamp) + ","
        descStr += str(self.UTCdate) + ","
        descStr += str(self.volume) + ","
        for i in range(3):
            for key in StampedState.__priceKeys:
                descStr += str(self.prices[i-1][key]) + ","
            if i is 3:
                descStr += "\n"


        return(descStr)

    def __eq__(self, other):
        return (self.timestamp == other.timestamp) and (self.instrumentName == other.instrumentName)

    def setVolume(self, volume):
        self.volume = volume;

    # the json file given must be at stage "candles"
    def setCHOLfromJSON(self, json_file, keyword):
        if keyword is StampedState.__ask:
            for key in StampedState.__priceKeys:
                self.prices[0][key] = float(json_file[keyword][key])
        elif keyword is StampedState.__bid:
            for key in StampedState.__priceKeys:
                self.prices[1][key] = float(json_file[keyword][key])
        elif keyword is StampedState.__mid:
            for key in StampedState.__priceKeys:
                self.prices[2][key] = float(json_file[keyword][key])
        elif keyword is StampedState.__volume:
            setVolume(int(json_file[keyword]))
        else:
            print("ERROR: Wrong keyword. Has to be ask, bid or mid")

    def getJSON(self):
        finalJSON = {}
        k = 0;
        finalJSON["databaseName"] = self.databaseName
        finalJSON["instrumentName"] = self.instrumentName
        finalJSON["granularity"] = self.granularity
        finalJSON["timestamp"] = int(self.timestamp)
        finalJSON["UTCdate"] = self.UTCdate.strftime('%Y-%m-%d %H:%M:%S')

        for i in {StampedState.__ask, StampedState.__bid, StampedState.__mid}:
            for j in StampedState.__priceKeys:
                finalJSON[i + '.' + j] =  self.prices[int(k)][j]
            k = k + 1

        return(finalJSON)

    def getVarList(self):
        finalList = []
        k = 0;
        finalList.append(int(self.timestamp))
        finalList.append(str(self.databaseName))
        finalList.append(str(self.instrumentName))
        finalList.append(str(self.granularity))
        finalList.append(self.UTCdate.strftime('%Y-%m-%d %H:%M:%S'))
        finalList.append(int(self.volume))

        for i in {StampedState.__ask, StampedState.__bid, StampedState.__mid}:
            for j in StampedState.__priceKeys:
                finalList.append(self.prices[int(k)][j])
            k = k + 1

        return(finalList)
