import oandapyV20 as oandapy
import unittest
import sys
sys.path.insert(0, 'C:/Users/Valentin/Documents/GitHub/MonitoringPlatform')

from fetchinstrumentoanda import *
from tools import Utility


class TestFetchInstrumentData(unittest.TestCase):
    NAME = "EUR_GBP"
    ACCOUNTID = "1337"
    GRAN = ["M5", "W"]
    API = oandapy.API(environment="practice",
                      access_token=Utility.getAccountToken(),
                      headers={'Accept-Datetime-Format': 'UNIX'})

    def test_init_number_of_dates(self):
        example = FetchInstrumentData(self.NAME, self.API, self.ACCOUNTID,
                                      self.GRAN)

        self.assertEqual(example.getNumberOfDates(), 0)


if __name__ == '__main__':
    unittest.main()
