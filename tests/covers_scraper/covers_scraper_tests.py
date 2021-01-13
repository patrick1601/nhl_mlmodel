import datetime as dt
from nhl_mlmodel.covers_scraper import covers_scraper
import unittest

class TestGetNhlDates(unittest.TestCase):
    def test_20172018_day7(self):
        """
        test nhl dates returned for the 20172018 season
        :return:
        """
        result = covers_scraper.get_nhl_dates(20172018)
        self.assertEqual(result[6], dt.datetime(2017,10,10,0,0))

if __name__ == '__main__':
    unittest.main()