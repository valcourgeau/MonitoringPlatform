import os
from urllib.parse import urlparse
from FetchOANDA import *
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class Utility:

    __granularityToSeconds = {
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

    #def __init__():

    @staticmethod
    def getSeconds(granularity):
        if(not granularity in Utility.__granularityToSeconds.keys()):
            print("Granularity not valid.")
        else:
            return(Utility.__granularityToSeconds[granularity])

    @staticmethod
    def getAddQuoteQuery(table_str):
        return "INSERT INTO " + table_str + """(timestamp, instrumentName, UTCdate, volume, ASK_C, ASK_H, ASK_O, ASK_L,
        BID_C, BID_H, BID_O, BID_L, MID_C, MID_H, MID_O, MID_L) VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    @staticmethod
    def getAccountID():
        return "101-004-5508575-001"

    @staticmethod
    def getAccountToken():
        return """84ee0448c71e837a162b3bca843ade9a-5783488d924d69861266a90653c9d898"""

    @staticmethod
    def getAssetUpdateTool(asset_str, database_info, granularity):
        if not isinstance(granularity, str):
            raise Exception('Should be given a string, nothing else!')
        else:
            assert k in Utility.__granularityToSeconds.keys()
            if database_info.getName() is 'oanda':
                temp = FetchInstrumentData(asset_str, database_info.tool,
                Utility.getAccountID(), granularity)

        return {granularity : temp}

    @staticmethod
    def getAssetUpdateToolDict(asset_str, database_info, granularity):
        assert isinstance(granularity, (list, tuple))
        #if granularity is float:
        #    raise Exception('Should be given a list, nothing else!')

        granToToolDict = {}
        if database_info.getName() is 'oanda':
            for k in granularity:
                assert k in Utility.__granularityToSeconds.keys()
                granToToolDict[k] = FetchInstrumentData(asset_str, database_info.tool,
                Utility.getAccountID(), k)

        return granToToolDict


    @staticmethod
    def connectHeroku(full_URL):

        url = urlparse(full_URL)
        dbname = url.path[1:]
        user = url.username
        password = url.password
        host = url.hostname
        port = url.port

        con = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                    )

        return(con)

    @staticmethod
    def get_table_col_names(con, table_str):

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

    @staticmethod
    def table_exists(con, table_str):

        exists = False
        try:
            cur = con.cursor()
            cur.execute("select exists(select relname from pg_class where relname='" + table_str + "')")
            exists = cur.fetchone()[0]
            print(exists)
            cur.close()
        except psycopg2.Error as e:
            print(e)
        return exists

    @staticmethod
    def create_price_table(con, table_str, override = False):

        if not override:
            if Utility.table_exists(con, table_str):
                raise Exception('Table already exists. Please drop it before creating new one. ' +
                'Otherwise, activate override.')
        else:
            Utility.drop_table(con, table_str)
            print("ALERT")
            cur = con.cursor()
            # cur.execute("DROP TABLE IF EXISTS " + "PRICE")
            cur.execute("""CREATE TABLE """ + table_str +
            """ (ID SERIAL PRIMARY KEY NOT NULL,
            Timestamp INTEGER NOT NULL,
            instrumentName VARCHAR(20) NOT NULL,
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

    @staticmethod
    def addQuoteListToDatabase(con, fetchInstr, table_str):
        if not isinstance(fetchInstr, FetchInstrumentData):
            raise Exception('addQuoteToDatabase: should be given an instance from FetchInstrumentData')
        else:
            if Utility.table_exists(con, table_str):
                cur = con.cursor()
                cur.executemany(Utility.getAddQuoteQuery(table_str), fetchInstr.getListofVarList())
                cur.close()
            else:
                raise Exception('addQuoteToDatabase: table ' + table_str + ' does not exist.')


    @staticmethod
    def drop_table(con, table_str):
        try:
            cur = con.cursor()
            instructionStr = "DROP TABLE IF EXISTS %s" % (table_str)
            cur.execute(instructionStr)
            cur.close()
            # commit the changes
            con.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            con.rollback()
            print(error)

    @staticmethod
    def close_connection(conn):
        if conn is not None:
            conn.close()
