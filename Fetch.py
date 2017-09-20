import os
import oandapyV20 as oandapy
import oandapyV20.endpoints.pricing as pricing
import oandapyV20.endpoints.instruments as instruments
import oandapyV20.endpoints.accounts as accounts
import configparser

from tools import Utility
from worker import *
from fetchinstrumentoanda import FetchInstrumentData

import time
from datetime import datetime, timedelta
import psycopg2

#DATABASE_URL_LOCAL = "postgres://mgixsptzrehvex:d13e83240446a75f2025cabd908c81fb31cc4d1b1448317a6a5bed33f0eabc58@ec2-54-243-185-132.compute-1.amazonaws.com:5432/dce76cagnnlqik"

config = configparser.ConfigParser()
config.read('oanda.cfg')

oanda = oandapy.API(environment="practice",
                    access_token=Utility.getAccountToken(),
                    headers={'Accept-Datetime-Format': 'UNIX'})
eurgbp = FetchInstrumentData("EUR_GBP", oanda, Utility.getAccountID(), "M5")
eurgbp.getHistoryFromToday(10)
eurusd = FetchInstrumentData("EUR_USD", oanda, Utility.getAccountID(), "S5")
eurusd.getHistoryFromToday(15)
print(eurusd.print_data())

# Creating price table
try:
    # read the connection parameters
    DATABASE_URL_LOCAL = os.environ['DATABASE_URL_HEROKU']

    # connect to the PostgreSQL server
    conn = Utility.connectHeroku(DATABASE_URL_LOCAL)

    oanda_info = DatabaseInfo('oanda', conn)
    print(oanda_info)
    # Creating EUR_USD asset
    print("Oanda printed")
    eurusd_info = AssetInfo("EUR_USD", oanda_info, ["S5", "M1"])
    print("asset info 1")
    eurgbp_info = AssetInfo("EUR_GBP", oanda_info, ["M5", "W"])

    print(eurusd_info)
    print(eurgbp_info)

    worker = Worker("Dave")
    print(worker)

    worker.addDatabase(oanda_info)
    worker.addAssetToDatabase(oanda_info, eurusd_info)
    worker.addAssetToDatabase(oanda_info, eurgbp_info)
    print("all right")
    worker.addGranularitytoAsset(oanda_info, eurgbp_info, ["M2"])
    print(eurgbp_info)

    Utility.clean_database(conn, True)
    Utility.create_price_table(conn, 'price', True)
    Utility.addQuoteListToDatabase(conn, eurgbp, 'price')
    Utility.addQuoteListToDatabase(conn, eurusd, 'price')
    print(Utility.get_all_table_names(conn))

    # commit the changes to database
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    conn.rollback()
    print(error)
finally:
    Utility.close_connection(conn)
