import unittest
import oandapyV20 as oandapy
from string import ascii_lowercase
import os
import sys
import psycopg2
from datetime import datetime, time, date
sys.path.insert(0, 'C:/Users/Valentin/Documents/GitHub/MonitoringPlatform')
from tools import Utility
from fetchinstrumentoanda import FetchInstrumentData


class TestToolsDB(unittest.TestCase):
    def test_get_history_from_given(self):
            """ Test fetching function from given date, set to be 23 March 2017:
            From    12:30 23/03/2017
            To      18:30 23/03/2017 """

            oanda = oandapy.API(environment="practice",
                                access_token=Utility.getAccountToken(),
                                headers={'Accept-Datetime-Format': 'UNIX'})
            eurgbp = FetchInstrumentData("EUR_GBP", oanda, 
                                        Utility.getAccountID(), "M5")

            d = date(2017, 3, 23)
            time_to = time(18, 30)
            time_from = time(12, 30)
            unix_timestamp_to = datetime.combine(d, time_to).timestamp()
            unix_timestamp_from = datetime.combine(d, time_from).timestamp()
            eurgbp.getHistoryFromGivenDate(5, unix_timestamp_from)
            eurgbp.print_data()


if __name__ == '__main__':
    unittest.main()
