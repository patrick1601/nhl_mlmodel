from bs4 import BeautifulSoup
import datetime as dt
from nhl_mlmodel.covers_scraper import helpers
import re
import requests
import sys
from typing import List

# Class Definitions
# ------------------

class NhlGame:
    """
    represents a game played in the nhl

    ...

    Parameters
    ----------
    date: dt.datetime
        date on which the game was played
    game_id: int
        game id to identify the game
    home_team: str
        home team abbreviation
    away_team: str
        away team abbreviation
    home_ml: int
        home moneyline odds
    home_score: int
        number of goals scored by the home team
    away_score: int
        number of goales score by the away team

    """
    def __init__(self, date: dt.datetime, game_id: int, home_team: str, away_team: str, home_ml: int,
                 home_score: int, away_score: int):
        self.date = date
        self.game_id = game_id
        self.home_team = home_team
        self.away_team = away_team
        self.home_ml = home_ml
        self.home_score = home_score
        self.away_score = away_score

# Function Definitions
# --------------------

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
    list[dt.datetime]
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

def nhl_games_date(date: dt.datetime) -> List[NhlGame]:
    """
    creates a list of NhlGame objects for all games played on the date provided

    ...

    Parameters
    ----------
    date: dt.datetime
        datetime object for which we want to

    Returns
    -------
    list[NhlGame]
        a list of NhlGame objects
    """
    games = []

    # retrieve the covers.com webpage for the date provided
    date = date.strftime('%Y-%m-%d')
    url = f'https://www.covers.com/sports/nhl/matchups?selectedDate={date}'
    resp = requests.get(url)

    # parse the page, and retrieve all the game boxes on the page
    scraped_games = BeautifulSoup(resp.text, features='html.parser').findAll('div', {'class': 'cmg_matchup_game_box'})

    # iterate through all the game boxes and retrieve required information for NhlGame object
    for g in scraped_games:
        game_id = g['data-event-id'] # game_id
        h_abv = g['data-home-team-shortname-search'] # home_team
        a_abv = g['data-away-team-shortname-search'] # away_team
        h_ml = g['data-game-odd'] # home moneyline

        try:
            h_score = g.find('div', {'class': 'cmg_matchup_list_score_home'}).get_text(strip=True) # home score
            a_score = g.find('div', {'class': 'cmg_matchup_list_score_away'}).get_text(strip=True) # away score
        except:  # If a score cannot be found leave as blank
            h_score = ''
            a_score = ''

        game = NhlGame(date, game_id, h_abv, a_abv, h_ml, h_score, a_score)

        games.append(game)



if __name__ == '__main__':
    dates = get_nhl_dates(20192020)

    games = []
    
    for d in dates:
        nhl_games_date(d)
        games += games