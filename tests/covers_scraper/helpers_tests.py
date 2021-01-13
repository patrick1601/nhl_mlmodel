import datetime as dt
from nhl_mlmodel.covers_scraper import helpers
import unittest

class TestRemoveDuplicates(unittest.TestCase):
    def test_ints(self):
        """
        test that integer duplicates can be removed
        :return:
        """
        data = [1,1,3,4,5,7,3,2,1,5,7,8]
        result = helpers.remove_duplicates(data)
        self.assertEqual(result, [1,3,4,5,7,2,8])
    def test_str(self):
        """
        test that string duplicates can be removed
        :return:
        """
        data = ['cat','dog','fish','dog','fish','cat','cow']
        result = helpers.remove_duplicates(data)
        self.assertEqual(result, ['cat','dog','fish','cow'])
    def test_date(self):
        """
        test that string duplicates can be removed
        :return:
        """
        data = [dt.date(2019,4,13), dt.date(2016,3,26), dt.date(2019,4,13), dt.date(2013,4,1)]
        result = helpers.remove_duplicates(data)
        self.assertEqual(result, [dt.date(2019,4,13), dt.date(2016,3,26), dt.date(2013,4,1)])

if __name__ == '__main__':
    unittest.main()
