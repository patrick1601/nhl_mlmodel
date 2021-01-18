
from nhl_mlmodel.nhl_scraper import nhl_scraper
import pandas as pd
import pickle
import sys
from time import sleep
from typing import List

# show full columns on dfs
pd.set_option('display.expand_frame_repr', False)


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

def pull_game_info(game_ids: List[int]) -> List[nhl_scraper.NhlGame]:
    """
    pulls all game_info for the provided game ids
    ...

    Parameters
    ----------
    game_ids: List[int]
        list of game ids to pull team stats for

    Returns
    -------
    games_info: List[nhl_scraper.NhlGame]
        list of NhlGame objects
    """

    # retrieve game by game info for every game in the game_ids list
    games_info = []

    for i in game_ids:
        game_i = nhl_scraper.scrape_game_info(i)
        games_info.append(game_i)

        if len(games_info) % 500 == 0:  # Progress bar
            print(str(len(games_info) / len(game_ids) * 100) + ' percent done retrieving game data/stats.')
    return games_info

def make_teams_df(team_stats: List[nhl_scraper.NhlTeam]) -> pd.DataFrame:
    """
        makes a dataframe from a list of NhlTeam objects
        ...

        Parameters
        ----------
        team_stats: List[nhl_scraper.NhlTeam]
            list of NhlTeam objects

        Returns
        -------
        teams_df: pd.DataFrame
            each row of dataframe represents stats for 1 team in 1 game. Therefore each game will have
            2 rows one for the home team and one for away.
        """

    teams_df = pd.DataFrame.from_records([t.to_dict() for t in team_stats])
    return teams_df

def make_goalies_df(goalie_stats: List[nhl_scraper.NhlGoalie]) -> pd.DataFrame:
    """
        makes a dataframe from a list of NhlGoalie objects
        ...

        Parameters
        ----------
        goalie_stats: List[nhl_scraper.NhlGoalie]
            list of NhlGoalie objects

        Returns
        -------
        goalies_df: pd.DataFrame
            each row of dataframe represents stats for 1 goalie in 1 game. Therefore each game will have
            at least 2 rows.
        """

    goalies_df = pd.DataFrame.from_records([g.to_dict() for g in goalie_stats])
    return goalies_df

def make_games_df(team_stats: List[nhl_scraper.NhlTeam]) -> pd.DataFrame:
    """
        main dataframe that will eventually get fed to the machine learning model
        ...

        Parameters
        ----------
        team_stats: List[nhl_scraper.NhlGoalie]
            list of NhlTeam objects

        Returns
        -------
        games_df: pd.DataFrame
            each row of dataframe represents 1 NHL game
        """


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
    if False:
        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/game_ids.pkl', 'rb') as f:
            game_ids = pickle.load(f)

        goalie_stats = pull_goalie_stats(game_ids)

        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/goalie_stats.pkl', 'wb') as f:
            pickle.dump(goalie_stats, f)

    # retrieve game by game information for all game ids pulled
    if True:
        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/game_ids.pkl', 'rb') as f:
            game_ids = pickle.load(f)

        games_info = pull_game_info(game_ids)

        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/games_info.pkl', 'wb') as f:
            pickle.dump(games_info, f)

    # make teams df
    if False:
        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/team_stats.pkl', 'rb') as f:
            team_stats_list = pickle.load(f)

        teams_df = make_teams_df(team_stats_list)

        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/team_df.pkl', 'wb') as f:
            pickle.dump(teams_df, f)

    # make goalies df
    if False:
        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/goalie_stats.pkl', 'rb') as f:
            goalie_stats_list = pickle.load(f)

        goalies_df = make_goalies_df(goalie_stats_list)

        with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/goalies_df.pkl', 'wb') as f:
            pickle.dump(goalies_df, f)