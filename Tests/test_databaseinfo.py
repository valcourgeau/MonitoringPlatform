import unittest
import sys
import os
sys.path.insert(0, 'C:/Users/Valentin/Documents/GitHub/MonitoringPlatform')
from databaseinfo import DatabaseInfo
from tools import Utility


class TestDatabaseInfo(unittest.TestCase):

    def test_eq(self):
        DATABASE_URL_LOCAL = os.environ['DATABASE_URL_HEROKU']
        conn = Utility.connectHeroku(DATABASE_URL_LOCAL)
        example = DatabaseInfo("Rock", conn)
        example2 = DatabaseInfo("Scissors", conn)

        self.assertNotEqual(example, example2)
        self.assertEqual(example, example)

    def test_hash(self):
        DATABASE_URL_LOCAL = os.environ['DATABASE_URL_HEROKU']
        conn = Utility.connectHeroku(DATABASE_URL_LOCAL)
        example = DatabaseInfo("Rock", conn)
        example3 = DatabaseInfo("Rock", conn)
        self.assertEqual(example, example3)

    def test_init_tool(self):
        DATABASE_URL_LOCAL = os.environ['DATABASE_URL_HEROKU']
        conn = Utility.connectHeroku(DATABASE_URL_LOCAL)
        example = DatabaseInfo("Rock", conn)
        self.assertEqual(None, example.getTool())


if __name__ == '__main__':
    unittest.main()
