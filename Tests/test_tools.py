import unittest
import os
import sys
sys.path.insert(0, 'C:/Users/Valentin/Documents/GitHub/MonitoringPlatform')
from tools import Utility


class TestTools(unittest.TestCase):
    TEST_TABLE_NAME_STR = "test_name"

    def test_table_name(self):
        # Checking test name of table
        self.assertEqual(self.TEST_TABLE_NAME_STR.find("("), -1)
        self.assertEqual(self.TEST_TABLE_NAME_STR.find(")"), -1)

    def test_granularity(self):
        time_dict = Utility.granularityToSeconds
        self.assertEqual(time_dict["S5"], Utility.getSeconds("S5"))
        self.assertEqual(time_dict["H1"], Utility.getSeconds("H1"))

        # Testing if it raises the correct error
        with self.assertRaises(ValueError):
                Utility.getSeconds("OK")

    def test_close_connection(self):
        DATABASE_URL_LOCAL = os.environ['DATABASE_URL_HEROKU']
        conn = Utility.connectHeroku(DATABASE_URL_LOCAL, printConn=False)

        self.assertEqual(conn.closed, 0)
        Utility.close_connection(conn)
        self.assertEqual(conn.closed, 1)

    def test_connect_heroku(self):
        DATABASE_URL_LOCAL = os.environ['DATABASE_URL_HEROKU']
        conn = Utility.connectHeroku(DATABASE_URL_LOCAL)

        # Closing database
        Utility.close_connection(conn)

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


if __name__ == '__main__':
    unittest.main()
