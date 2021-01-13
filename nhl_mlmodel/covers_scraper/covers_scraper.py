from bs4 import BeautifulSoup
import datetime as dt
from nhl_mlmodel.covers_scraper import helpers
import re
import requests
import sys
from typing import List

def get_nhl_dates(season: int) -> List[dt.datetime]:
    """
    returns a list of all dates NHL games were played in the specified season

    ...

    Parameters
    ----------
    season: int
        the two years of the season that nhl dates will be retrieved for (ex. 20192020)

    Returns
    -------
    list
        a list of datetime objects
    """
    # Determine the two years that NHL games were played for that season
    year1: int = int(str(season)[:4])
    year2: int = int(str(season)[4:])

    # Form the url and get response from hockey-reference.com
    url: str = f'https://www.hockey-reference.com/leagues/NHL_{year2}_games.html'
    resp: type = requests.get(url)

    # Find all the days games were played for year1 and year 2.
    days1: List[str] = re.findall(f'html">({year1}.*?)</a></th>', resp.text)
    days2: List[str] = re.findall(f'html">({year2}.*?)</a></th>', resp.text)
    days: List[str] = days1 + days2

    # Remove duplicates and convert strings to datetime
    days = helpers.remove_duplicates(days)
    dates: List[dt.datetime] = [dt.datetime.strptime(d, '%Y-%m-%d') for d in days]

    print(f'Number of days NHL regular season played in {season}: ', len(dates))
    return dates
