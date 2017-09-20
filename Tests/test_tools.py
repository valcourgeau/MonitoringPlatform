import unittest
from string import ascii_lowercase
import os
import sys
import psycopg2
sys.path.insert(0, 'C:/Users/Valentin/Documents/GitHub/MonitoringPlatform')
from tools import Utility


class TestTools(unittest.TestCase):
    TEST_TABLE_NAME_STR = "test_name"
    TEST_DATABASE_URL = "postgres://mgixsptzrehvex:d13e83240446a75f2" + \
                        "025cabd908c81fb31cc4d1b1448317a6a5be" + \
                        "d33f0eabc58@ec2-54-243-185-132.compute-1" + \
                        ".amazonaws.com:5432/" + \
                        "dce76cagnnlqik"

    def test_connect_heroku(self):
        database_url_local = os.environ['DATABASE_URL_HEROKU']
        conn = Utility.connectHeroku(database_url_local)
        self.assertEqual(conn.closed, 0)
        # Closing database connection
        Utility.close_connection(conn)

    def test_create_test_table(self):
        database_url_local = os.environ['DATABASE_URL_HEROKU']
        conn = Utility.connectHeroku(database_url_local)

        # Deleting the table vendors
        Utility.drop_table(conn, "vendors")
        # Creating tables
        TestTools.create_test_tables()

        self.assertTrue(Utility.table_exists(conn, 'vendors'))
        self.assertFalse(Utility.table_exists(conn, 'parts'))
        self.assertFalse(Utility.table_exists(conn, 'part_drawings'))

        # Closing database connection
        Utility.close_connection(conn)

    def test_table_name(self):
        # Checking test name of table
        self.assertEqual(self.TEST_TABLE_NAME_STR.find("("), -1)
        self.assertEqual(self.TEST_TABLE_NAME_STR.find(")"), -1)
    
    def test_get_seconds(self):
        time_dict = Utility.granularityToSeconds
        self.assertEqual(time_dict["S5"], Utility.getSeconds("S5"))
        self.assertEqual(time_dict["H1"], Utility.getSeconds("H1"))
        self.assertEqual([time_dict[index] for index in ["H1", "W"]],
                         Utility.getSeconds(["H1", "W"]))
        # Testing if it raises the correct error
        with self.assertRaises(ValueError):
                Utility.getSeconds("OK")
    
    def test_close_connection(self):
        DATABASE_URL_LOCAL = os.environ['DATABASE_URL_HEROKU']
        conn = Utility.connectHeroku(DATABASE_URL_LOCAL, printConn=False)
    
        self.assertEqual(conn.closed, 0)
        Utility.close_connection(conn)
        self.assertEqual(conn.closed, 1)
    
    def test_get_add_quote_query(self):
        # Checking the number of inputs and
        # number of inputs in the resulting str
    
        inputs_str = Utility.getAddQuoteQuery(self.TEST_TABLE_NAME_STR)
        first_str = inputs_str[inputs_str.find("("):inputs_str.find(")")]
        first_str = first_str.split()
        second_str = inputs_str[
                     inputs_str.find("(", inputs_str.find("(")+1):
                     inputs_str.find(")", inputs_str.find(")")+1)
                     ]
        second_str = second_str.split()
        self.assertEqual(len(first_str), len(second_str))
    
    def test_get_database_url(self):
        database_url = Utility.get_database_url()
        self.assertEqual(database_url[0], self.TEST_DATABASE_URL[0])
        self.assertEqual(len(database_url), len(self.TEST_DATABASE_URL))
    
        # Checking the order of appearence of all lowercase letters in url
        for char in ascii_lowercase:
            self.assertEqual(database_url.find(char),
                             self.TEST_DATABASE_URL.find(char))
    
        # Checking str equality of URLs
        self.assertEqual(Utility.get_database_url(), self.TEST_DATABASE_URL)
    
    def test_is_list_granularity(self):
        self.assertTrue(Utility.is_list_in_granularity("S5"))
        self.assertFalse(Utility.is_list_in_granularity("TEST"))
        self.assertTrue(Utility.is_list_in_granularity(["S5", "M5", "W"]))
        self.assertFalse(Utility.is_list_in_granularity(["TEST"]))
    
    def test_table_exists(self):
        conn = Utility.connectHeroku(Utility.get_database_url())

        # Deleting the table vendors
        Utility.drop_table(conn, "vendors")
        # Create test tables
        TestTools.create_test_tables()

        self.assertTrue(Utility.table_exists(conn, 'vendors'))
        self.assertFalse(Utility.table_exists(conn, 'test'))
        Utility.close_connection(conn)
    
    def test_get_table_col_names(self):
        part_drawings_col_names = ['vendor_id', 'vendor_name']
        conn = Utility.connectHeroku(Utility.get_database_url())
        # Deleting the table vendors
        Utility.drop_table(conn, "vendors")
        # Create test tables
        TestTools.create_test_tables()

        col_names = Utility.get_table_col_names(conn, 'vendors')
        for index in range(len(part_drawings_col_names)):
            self.assertEqual(col_names[index], part_drawings_col_names[index])
        conn.commit()
        Utility.close_connection(conn)
    
    def test_drop_table(self):
        conn = Utility.connectHeroku(Utility.get_database_url())

        # Deleting the table vendors
        Utility.drop_table(conn, "vendors")
        # Create test tables
        TestTools.create_test_tables()

        self.assertTrue(Utility.table_exists(conn, 'vendors'))
        Utility.drop_table(conn, 'vendors')
        self.assertFalse(Utility.table_exists(conn, 'vendors'))
        conn.commit()
        Utility.close_connection(conn)

    def test_get_all_table_names(self):
        conn = Utility.connectHeroku(Utility.get_database_url())

        # Deleting the table vendors
        Utility.drop_table(conn, "vendors")
        # Create test tables
        TestTools.create_test_tables()

        true_names = ["vendors"]
        table_names = Utility.get_all_table_names(conn)
        for index in range(len(table_names)):
            self.assertEqual(table_names[index], true_names[index])

    def test_create_price_table_creation_only(self):
        """ Test the function Utility.create_price_table on creation only """
        conn = Utility.connectHeroku(Utility.get_database_url())
        Utility.create_price_table(conn, 'price', True)
        self.assertTrue(Utility.table_exists(conn, 'price'))

    def test_create_price_table_exception(self):
        """ Test the function Utility.create_price_table on exception only """
        conn = Utility.connectHeroku(Utility.get_database_url())
        Utility.create_price_table(conn, 'price', True)
        with self.assertRaises(Exception):
                Utility.create_price_table(conn, 'price')
    
    def test_clean_database(self):
        conn = Utility.connectHeroku(Utility.get_database_url())

        for table in Utility.get_all_table_names(conn):
            Utility.drop_table(conn, table)
        # Deleting the table vendors
        Utility.drop_table(conn, "vendors")
        # Create test tables
        TestTools.create_test_tables()
        self.assertEqual(len(Utility.get_all_table_names(conn)), 1)
        Utility.clean_database(conn)
        self.assertEqual(len(Utility.get_all_table_names(conn)), 0)
    

    @classmethod
    def create_test_tables(cls):
        # """ create tables in the PostgreSQL database"""
        commands = """
            CREATE TABLE vendors (
                vendor_id SERIAL PRIMARY KEY,
                vendor_name VARCHAR(255) NOT NULL
            )
            """
        conn = None
        try:
            # connect to the PostgreSQL server
            database_url_local = os.environ['DATABASE_URL_HEROKU']
            conn = Utility.connectHeroku(database_url_local)
            cur = conn.cursor()
            # create table one by one
            cur.execute(commands)

            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    ##################################        
    # TESTS FOR INTERFACE WITH OANDA #
    ##################################

    

if __name__ == '__main__':
    unittest.main()
