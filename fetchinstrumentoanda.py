import oandapyV20.endpoints.instruments as instruments
# import oandapyV20.endpoints.accounts as accounts
# import configparser
# import traceback

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from stampedstate import StampedState
# from tools import Utility


class FetchInstrumentData:
    __MAX_COUNT = 5000
    __MAX_NUMBER_STARTING_HISTORY = 100

    def __init__(self, instrumentName, api, accountID, granularity):
        self.api = api
        self.databaseName = 'oanda'
        self.instrumentName = instrumentName
        self.accountID = accountID
        self.granularity = granularity
        self.priceDict = {}
        self.dateListSubmittedToDataBase = []
        self.hasPulledData = False
        self.lastPulledDataTimestamp = 0.0

    def __str__(self):
        return("Account ID: " + self.accountID + "\n" +
               "Instrument Name: " + self.instrumentName + "\n" +
               "Granularity: " + self.granularity + "\n")

    def getInstrumentName(self):
        return(self.instrumentName)

    def submitStampedStateToDatabase(self, cur, stampedstate):
        if(not isinstance(stampedstate, StampedState)):
            raise ValueError('Input should be from class StampedState!')

    def GetCSVFile(self):
        # Note: Implement customisable column names
        if(not self.hasPulledData):
            print("Data needs to be imported/pulled first.")
            return
        with open(str(self.instrumentName) + "_" + str(self.granularity) + "_"
                  + str(len(self.priceDict)) + ".csv", 'w') as csvFile:
            print("Index," + StampedState.rowNames(), file=csvFile)
            length = len(self.priceDict)
            for k in range(length):
                print(str(k+1) + "," + str(self.priceDict[k]), file=csvFile)

        return csvFile

    def getHistoryFromToday(self, numberPoints):
        # Return numberPoints datapoints for the given instrument from the most
        # recent trade date

        # Ugly but it works...
        from tools import Utility
        toDate = Utility.getLondonUNIXDate()
        self.getHistoryFromGivenDate(numberPoints, toDate)

    def getLastPulledDataTimestamp(self):
        return self.lastPulledDataTimestamp

    def updateHistory(self):
        if(self.hasPulledData):
            print("Data was pulled %d to %d" % (self.lastPulledDataTimestamp,
                                                Utility.getLondonUNIXDate()))
            self.getHistoryFromAndTo(self.lastPulledDataTimestamp,
                                     Utility.getLondonUNIXDate())
        else:
            self.getHistoryFromGivenDate(
                            FetchInstrumentData.__MAX_NUMBER_STARTING_HISTORY,
                            Utility.getLondonUNIXDate())

    def getNumberOfDates(self):
        return len(self.priceDict.keys())

    def addQuote(self, quoteInfo, checkLastDate):
        # take a JSON Quote and set up a StampedState object to be saved

        if(not quoteInfo['complete'] is True):
            print("ERROR: Data has not been correctly loaded.")
            return

        timestamp = float(quoteInfo['time'])
        state = StampedState(timestamp, self.databaseName,
                             self.instrumentName, self.granularity)
        state.setVolume(quoteInfo['volume'])
        state.setCHOLfromJSON(quoteInfo, "ask")
        state.setCHOLfromJSON(quoteInfo, "bid")
        state.setCHOLfromJSON(quoteInfo, "mid")

        if(checkLastDate):
            if(timestamp > self.lastPulledDataTimestamp):
                self.priceDict[self.getNumberOfDates()] = state
        else:
            self.priceDict[self.getNumberOfDates()] = state

    def resetPriceDict(self):
        # empty price dictionary
        self.priceDict = {}

    def getJSONdict(self):
        results = tuple(v.getJSON() for k, v in self.priceDict.items())
        # for k, v in self.priceDict.items():
        #     results.append(v.getJSON())
        # return({k: v.getJSON() for k, v in self.priceDict.items()})
        return(results)

    def getListofVarList(self):
        results = tuple(v.getVarList() for k, v in self.priceDict.items())
        return(results)

    def printData(self):
        results = self.getListofVarList()

        for x in results:
            print(x)

    def getHistoryFromGivenDate(self, numberPoints, UNIXtimestamp):
        # loads the numberPoints points from the given instrument
        # form given UNIX timestamp

        print("Loading history from server from %d for %d points." %
              (UNIXtimestamp, numberPoints))

        toDate = UNIXtimestamp
        index = numberPoints

        # Create the request parameters:
        paramsRequest = {}
        paramsRequest["granularity"] = self.granularity
        paramsRequest["to"] = toDate
        paramsRequest["price"] = "MBA"  # M = mid, B = bid, A = ask
        paramsRequest["insertFirst"] = True

        self.lastPulledDataTimestamp = paramsRequest["to"]

        # Number of batches:
        maxJ = int(numberPoints/FetchInstrumentData.__MAX_COUNT)

        if(not (maxJ * FetchInstrumentData.__MAX_COUNT == numberPoints)):
            maxJ += 1

        for j in range(maxJ):
            # Determine number of datapoints to fetch this time
            if ((j + 1) * FetchInstrumentData.__MAX_COUNT > numberPoints):
                count = numberPoints - j * FetchInstrumentData.__MAX_COUNT
            else:
                count = FetchInstrumentData.__MAX_COUNT

            # Update parameters and request
            paramsRequest["count"] = count
            r = instruments.InstrumentsCandles(instrument=self.instrumentName,
                                               params=paramsRequest)
            print(r)
            self.api.request(r)

            responseFile = r.response["candles"]

            if(index - count < 0):
                index = 0
            else:
                index -= count

            # Setting the next starting date:
            paramsRequest["to"] = responseFile[0]["time"]
            # update "from" date to the last one picked

            # Save data into data structure
            for i in range(count):
                quoteInfo = responseFile[i]
                self.addQuote(quoteInfo, False)

        self.hasPulledData = True
        print("History loaded from server.")

    def getHistoryFromAndTo(self, UNIXtimestamp_from, UNIXtimestamp_to):
        # previousDictLength = len(self.priceDict.keys())
        index = (UNIXtimestamp_to-UNIXtimestamp_from)
        index = index / Utility.getSeconds(self.granularity)
        index = int(index)
        numberPoints = index

        # Create the request parameters:
        paramsRequest = {}
        paramsRequest["granularity"] = self.granularity
        paramsRequest["to"] = UNIXtimestamp_to
        paramsRequest["price"] = "MBA"  # M = mid, B = bid, A = ask
        paramsRequest["insertFirst"] = True

        print("History updated from %d to %d" % (UNIXtimestamp_from,
                                                 UNIXtimestamp_to))

        # Number of batches:
        maxJ = int(numberPoints/FetchInstrumentData.__MAX_COUNT)

        # Add one batch if it does not add up perfectly
        if(not (maxJ * FetchInstrumentData.__MAX_COUNT == numberPoints)):
            maxJ += 1

        for j in range(maxJ):
            # Determine number of datapoints to fetch this time
            if ((j + 1) * FetchInstrumentData.__MAX_COUNT > numberPoints):
                count = numberPoints - j * FetchInstrumentData.__MAX_COUNT
            else:
                count = FetchInstrumentData.__MAX_COUNT

            # Update parameters and request
            paramsRequest["count"] = count
            r = instruments.InstrumentsCandles(instrument=self.instrumentName,
                                               params=paramsRequest)
            rv = self.api.request(r)
            responseFile = r.response["candles"]

            if(index - count < 0):
                index = 0
            else:
                index -= count

            # Setting the next starting date:
            paramsRequest["to"] = responseFile[0]["time"]
            # update "from" date to the last one picked

            # Save data into data structure
            for i in range(count):
                quoteInfo = responseFile[i]
                self.addQuote(quoteInfo, True)

            self.lastPulledDataTimestamp = UNIXtimestamp_to
            print("History updated: %d points" % numberPoints)
