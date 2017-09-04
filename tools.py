import os
from urllib.parse import urlparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime, timedelta

from fetchinstrumentoanda import *


class Utility:
    __TIMEDIFF_LONDON_NEW_YORK = 5*60*60  # number of secs between NYC / London

    granularityToSeconds = {
        "S5": 5,
        "S10": 10,
        "S15": 15,
        "S30": 30,
        "M1": 60,
        "M2": 2*60,
        "M3": 3*60,
        "M4": 4*60,
        "M5": 5*60,
        "M10": 10*60,
        "M15": 15*60,
        "M30": 30*60,
        "H1": 60*60,
        "H2": 2*60*60,
        "H3": 3*60*60,
        "H4": 4*60*60,
        "H6": 6*60*60,
        "H8": 8*60*60,
        "H12": 12*60*60,
        "D": 24*60*60,
        "W": 5*24*60*60,
        "M": 23*60*60
    }

    def __init__():
        pass

    @classmethod
    def getSeconds(cls, granularity):
        if(granularity not in Utility.granularityToSeconds.keys()):
            raise ValueError("""Utility.getSeconds: Granularity
                            token not valid.""")
        else:
            return(Utility.granularityToSeconds[granularity])

    @classmethod
    def getAddQuoteQuery(cls, table_str):
        return "INSERT INTO " + table_str + """(timestamp, databaseName,
               instrumentName, granularity, UTCdate, volume, ASK_C, ASK_H,
               ASK_O, ASK_L,
               BID_C, BID_H, BID_O, BID_L, MID_C, MID_H, MID_O, MID_L) VALUES
               (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
               %s, %s)"""

    @classmethod
    def getAccountID(cls):
        return "101-004-5508575-001"

    @classmethod
    def getAccountToken(cls):
        return """c445061915f8c3c7ccf57580a0512ce2-
        9743bd23f0405617aa5ca5074396dab8"""

    @classmethod
    def getAssetUpdateTool(cls, asset_str, database_info, granularity):
        if not isinstance(granularity, str):
            raise Exception('Should be given a string, nothing else!')
        else:
            assert granularity in Utility.granularityToSeconds.keys()
            if database_info.getName() is 'oanda':
                temp = FetchInstrumentData(asset_str, database_info.tool,
                                           Utility.getAccountID(), granularity)
        return {granularity: temp}

    @classmethod
    def getAssetUpdateToolDict(cls, asset_info, database_info, granularity):
        # assert isinstance(database_info, DatabaseInfo)
        # assert isinstance(asset_info, AssetInfo)
        assert isinstance(granularity, (list, tuple))
        # if granularity is float:
        #    raise Exception('Should be given a list, nothing else!')

        granToToolDict = {}
        if database_info.getName() is 'oanda':
            for k in granularity:
                assert k in Utility.granularityToSeconds.keys()
                granToToolDict[k] = FetchInstrumentData(asset_info.getName(),
                                                        database_info.tool,
                                                        Utility.getAccountID(),
                                                        k)

        return granToToolDict

    @classmethod
    def connectHeroku(cls, full_URL, printConn=False):
        assert printConn in (True, False)
        url = urlparse(full_URL)
        dbname = url.path[1:]
        user = url.username
        password = url.password
        host = url.hostname
        port = url.port

        if printConn:
            print("=========== DATABASE - HEROKU ===========")
            print("User: " + str(user))
            print("Host: " + str(host))
            print("Port: " + str(port))
            print("Password: " + str(password))

        conn = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port)

        return(conn)

    @classmethod
    def get_table_col_names(cls, con, table_str):

        col_names = []
        try:
            cur = con.cursor()
            cur.execute("select * from " + table_str + " LIMIT 0")
            for desc in cur.description:
                col_names.append(desc[0])
            cur.close()
        except psycopg2.Error as e:
            print(e)

        return(col_names)

    @classmethod
    def table_exists(cls, con, table_str):

        exists = False
        try:
            cur = con.cursor()
            cur.execute("""select exists(select relname
                        from pg_class where relname='""" + table_str + "')")
            exists = cur.fetchone()[0]
            print(exists)
            cur.close()
        except psycopg2.Error as e:
            print(e)
        return exists

    @classmethod
    def create_price_table(cls, con, table_str, override=False):

        if Utility.table_exists(con, table_str):
            if not override:
                raise Exception("""Table already exists. Please drop it before
                                creating new one. """ +
                                'Otherwise, activate override.')
            else:
                print("ALERT: dropping table.")
                Utility.drop_table(con, table_str)
                Utility.create_price_table(con, table_str)
        else:
            cur = con.cursor()
            # cur.execute("DROP TABLE IF EXISTS " + "PRICE")
            cur.execute("""CREATE TABLE """ + table_str +
                        """ (ID SERIAL PRIMARY KEY NOT NULL,
                        Timestamp INTEGER NOT NULL,
                        databaseName VARCHAR(20) NOT NULL,
                        instrumentName VARCHAR(20) NOT NULL,
                        granularity VARCHAR(5) NOT NULL,
                        UTCdate VARCHAR(20) NOT NULL,
                        volume INTEGER,
                        ASK_C REAL NOT NULL,
                        ASK_H REAL NOT NULL,
                        ASK_O REAL NOT NULL,
                        ASK_L REAL NOT NULL,
                        BID_C REAL NOT NULL,
                        BID_H REAL NOT NULL,
                        BID_O REAL NOT NULL,
                        BID_L REAL NOT NULL,
                        MID_C REAL NOT NULL,
                        MID_H REAL NOT NULL,
                        MID_O REAL NOT NULL,
                        MID_L REAL NOT NULL)""")

            # close communication with the PostgreSQL database server
            cur.close()

    @classmethod
    def addQuoteListToDatabase(cls, con, fetchInstr, table_str):
        if not isinstance(fetchInstr, FetchInstrumentData):
            raise Exception('addQuoteToDatabase:' +
                            """should be given an instance
                            from FetchInstrumentData""")
        else:
            if Utility.table_exists(con, table_str):
                cur = con.cursor()
                cur.executemany(Utility.getAddQuoteQuery(table_str),
                                fetchInstr.getListofVarList())
                fetchInstr.resetPriceDict()
                cur.close()
            else:
                raise Exception('addQuoteToDatabase: table ' + table_str +
                                ' does not exist.')

    @classmethod
    def drop_table(cls, con, table_str):
        try:
            cur = con.cursor()
            instructionStr = "DROP TABLE IF EXISTS {}".format(table_str)
            cur.execute(instructionStr)
            cur.close()
            # commit the changes
            con.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            con.rollback()
            print(error)

    @classmethod
    def getLondonUNIXDate(cls):
        return datetime.utcnow().timestamp()-Utility.__TIMEDIFF_LONDON_NEW_YORK

    @classmethod
    def close_connection(cls, conn):
        if conn is not None:
            conn.close()
