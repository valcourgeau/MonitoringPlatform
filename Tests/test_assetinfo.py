import unittest
import sys
sys.path.insert(0, 'C:/Users/Valentin/Documents/GitHub/MonitoringPlatform')
from assetinfo import AssetInfo


class TestAssetInfo(unittest.TestCase):

    def test_last_update_time_init(self):
        example = AssetInfo("Rock", None, "M5")
        print(example.getLastUpdateTime())
        self.assertEqual(example.getLastUpdateTime(), 0)


if __name__ == '__main__':
    unittest.main()
