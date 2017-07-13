import csv
import json
import oandapyV20 as oandapy
import oandapyV20.endpoints.pricing as pricing
import oandapyV20.endpoints.instruments as instruments
import oandapyV20.endpoints.accounts as accounts
import configparser
import pandas as pd
from Utility import *
from Worker import *

import time
from datetime import datetime, timedelta
import psycopg2

from FetchOANDA import *

#DATABASE_URL_LOCAL = "postgres://mgixsptzrehvex:d13e83240446a75f2025cabd908c81fb31cc4d1b1448317a6a5bed33f0eabc58@ec2-54-243-185-132.compute-1.amazonaws.com:5432/dce76cagnnlqik"

config = configparser.ConfigParser()
config.read('oanda.cfg')

oanda = oandapy.API(environment = "practice",
access_token = Utility.getAccountToken(),
headers={'Accept-Datetime-Format': 'UNIX'})
eurgbp = FetchInstrumentData("EUR_GBP", oanda, Utility.getAccountID(), "S5")
eurgbp.getHistoryFromToday(20)
eurusd = FetchInstrumentData("EUR_USD", oanda, Utility.getAccountID(), "S5")
eurusd.getHistoryFromToday(15)
print(eurusd.getListofVarList())



#time.sleep(20)
#eurusd.updateHistory()

#
#print(eurusd.getJSONdict()[1])


def create_tables():
    commands = (
    """
    CREATE TABLE vendors (
        vendor_id SERIAL PRIMARY KEY,
        vendor_name VARCHAR(255) NOT NULL
    )
    """,
    """ CREATE TABLE parts (
            part_id SERIAL PRIMARY KEY,
            part_name VARCHAR(255) NOT NULL
            )
    """,
    """
    CREATE TABLE part_drawings (
            part_id INTEGER PRIMARY KEY,
            file_extension VARCHAR(5) NOT NULL,
            drawing_data BYTEA NOT NULL,
            FOREIGN KEY (part_id)
            REFERENCES parts (part_id)
            ON UPDATE CASCADE ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE vendor_parts (
            vendor_id INTEGER NOT NULL,
            part_id INTEGER NOT NULL,
            PRIMARY KEY (vendor_id , part_id),
            FOREIGN KEY (vendor_id)
                REFERENCES vendors (vendor_id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (part_id)
                REFERENCES parts (part_id)
                ON UPDATE CASCADE ON DELETE CASCADE
    )
    """)
    conn = None

# Creating price table
try:
    # read the connection parameters
    DATABASE_URL_LOCAL = os.environ['DATABASE_URL_HEROKU']

    # connect to the PostgreSQL server
    conn = Utility.connectHeroku(DATABASE_URL_LOCAL)

    oanda_info = DatabaseInfo('oanda', conn)
    print(oanda_info)
    # Creating EUR_USD asset

    eurusd_info = AssetInfo("EUR_USD", oanda_info, ["S5", "M1"])
    eurgbp_info = AssetInfo("EUR_GBP", oanda_info, ["M5", "W"])
    print(eurusd_info)

    worker = Worker("Dave")
    
    worker.addDatabase(oanda_info)
    worker.addAssetOnDatabase('oanda', eurusd_info)
    worker.addAssetOnDatabase('oanda', eurgbp_info)
    print("ok")
    print(worker)
    worker.addGranularitytoAsset('oanda', 'EUR_GBP', ["M2"])
    print(worker)

    Utility.create_price_table(conn, 'price', True)
    Utility.addQuoteListToDatabase(conn, eurgbp, 'price')
    Utility.addQuoteListToDatabase(conn, eurusd, 'price')

    # commit the changes
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    conn.rollback()
    print(error)
finally:
    Utility.close_connection(conn)


#create_tables()
