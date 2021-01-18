
from nhl_mlmodel.nhl_scraper import nhl_scraper
import pickle
import sys
from time import sleep
from typing import List


def pull_game_ids(first_year: int=2010, last_year: int=2020) -> List[int]:
    """
    pulls all nhl game ids between the specified dates
    ...

    Parameters
    ----------
    first_year: int
        first year to retrieve game ids
    last_year: int
        last year to retrieve game ids

    Returns
    -------
    game_ids: str
        team abbreviation
    """
    years = list(range(first_year, last_year))  # Create list of years for which we want data

    # Create year for the get_game_ids() function argument in the format 20192020
    game_ids_url_years = []

    for i in years:
        j = str(i) + str(i + 1)
        game_ids_url_years.append(j)

    # Run for loop to retrieve game IDs for all seasons required
    game_ids = []
    for i in game_ids_url_years:

        if len(game_ids) % 500 == 0:  # Progress bar
            print(str(len(game_ids) / len(game_ids_url_years) * 100) + ' percent done retrieving game ids.')

        try:
            game_ids = game_ids + nhl_scraper.get_game_ids(i)

        except KeyError:
            print(str('*************Not able to retrieve: ' + str(i) + ' games due to KeyError************'))
            continue

    return game_ids

def pull_team_stats(game_ids: List[int]) -> List[nhl_scraper.NhlTeam]:
    """
    pulls all team stats for the provided game ids
    ...

    Parameters
    ----------
    game_ids: List[int]
        list of game ids to pull team stats for

    Returns
    -------
    team_stats: List[nhl_scraper.NhlTeam]
        list of NhlTeam objects
    """

    # retrieve game by game stats for every game in the game_ids list
    team_stats = []

    for i in game_ids:
        stats_i = nhl_scraper.scrape_team_stats(i)
        team_stats += stats_i

        if len(team_stats) % 500 == 0:  # Progress bar
            print(str(0.5 * len(team_stats) / len(game_ids) * 100) + ' percent done retrieving game data/stats.')
        #sleep(1)  # Sleep to avoid max retry errors

    return team_stats

def pull_goalie_stats(game_ids: List[int]) -> List[nhl_scraper.NhlGoalie]:
    """
        pulls all goalie stats for the provided game ids
        ...

        Parameters
        ----------
        game_ids: List[int]
            list of game ids to pull team stats for

        Returns
        -------
        goalie_stats: List[nhl_scraper.NhlGoalie]
            list of NhlGoalie objects
        """

    goalie_stats=[]
    for i in game_ids:
        goalies_i = nhl_scraper.scrape_goalie_stats(i)
        goalie_stats += goalies_i

        if len(goalie_stats) % 250 == 0:  # Progress bar # todo fix progress bar to account for more goalies than game ids
            print(str(0.5 * len(goalie_stats) / len(game_ids) * 100) + ' percent done retrieving goalie data.')

    return goalie_stats


if __name__ == '__main__':
    # pull all game ids between 2010-2020
    if False:
        game_ids = pull_game_ids(2010, 2020)
        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/game_ids.pkl', 'wb') as f:
            pickle.dump(game_ids, f)

    # retrieve team game by game stats for all game ids pulled
    if False:
        team_stats = pull_team_stats(game_ids)
        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/team_stats.pkl', 'wb') as f:
            pickle.dump(team_stats, f)

    # retrieve goalie game by game stats for all game ids pulled
    if True:
        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/game_ids.pkl', 'rb') as f:
            game_ids = pickle.load(f)

        goalie_stats = pull_goalie_stats(game_ids)

        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/goalie_stats.pkl', 'wb') as f:
            pickle.dump(goalie_stats, f)