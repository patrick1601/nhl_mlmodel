import json
from nhl_mlmodel.nhl_scraper import nhl_scraper
from nhl_mlmodel.power_rankings import power_rankings
from nhl_mlmodel.process_data import process_data
from nhl_mlmodel.update_data import helpers
import numpy as np
import pandas as pd
import pickle
import requests
import sys
from typing import List

def update_game_ids(season: int, games_df: pd.DataFrame) -> List[int]:
    """
    retrieves game ids that have happened and are not in the main dataframe
    ...

    Parameters
    ----------
    season: int
        season for which to search for new game ids (eg. 20202021)
    games_df: pd.DataFrame
        current games dataframe

    Returns
    -------
    new_game_ids: List[int]
        new game ids to be added
    """
    # get current game ids
    current_game_ids = games_df['game_id'].tolist()

    # Retrieve all game_ids for the season
    season_game_ids = nhl_scraper.get_game_ids(season)

    # Return new_game_ids which is a list of game ids that are currently not present
    # in our game_ids list
    new_game_ids = [list(set(season_game_ids).difference(current_game_ids))]
    new_game_ids = new_game_ids[0]
    new_game_ids = helpers.remove_duplicates(new_game_ids)

    # Delete game ids from list that have not been played yet
    for id in new_game_ids:
        # Retrieve date for id
        url = f'https://statsapi.web.nhl.com/api/v1/game/{str(id)}/feed/live'
        resp = requests.get(url)
        json_data = json.loads(resp.text)
        id_status = json_data['gameData']['status']['abstractGameState']

        # Delete if game if status is not Final
        if id_status != 'Final':
            new_game_ids = [x for x in new_game_ids if x != id]
        else:
            continue

    print(str(len(new_game_ids)) + ' game ids to update.')
    print(new_game_ids)

    return new_game_ids

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

def make_games_df(games_info: List[nhl_scraper.NhlGame]) -> pd.DataFrame:
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
    games_df = pd.DataFrame.from_records([g.to_dict() for g in games_info])
    return games_df

def convert_numerical(teams_df: pd.DataFrame, goalies_df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """
        convert non-numerical features in dataframes to numerical
        ...

        Parameters
        ----------
        teams_df: pd.DataFrame
            dataframe containing team stats

        goalies_df: pd.DataFrame
            dataframe containing goalies stats

        Returns
        -------
        teams_numerical_df: pd.DataFrame
            teams_df converted to numerical
        goalies_numerical_df: pd.DataFrame
            goalies_numerical_df: pd.DataFrame
        """
    # CONVERT OBJECTS TO NUMERICAL
    teams_numerical_df = teams_df.copy()
    goalies_numerical_df = goalies_df.copy()
    # powerPlayPercentage
    teams_numerical_df['powerPlayPercentage'] = teams_numerical_df['powerPlayPercentage'].astype(float)
    # faceOffWinPercentage
    teams_numerical_df['faceOffWinPercentage'] = teams_numerical_df['faceOffWinPercentage'].astype(float)
    # Convert Goalie timeOnIce to Minutes
    goalies_numerical_df['timeOnIce'] = goalies_numerical_df['timeOnIce'].map(helpers.convert_minutes)

    # Reset index
    teams_numerical_df.reset_index(inplace=True)
    goalies_numerical_df.reset_index(inplace=True)

    return teams_numerical_df, goalies_numerical_df

def add_pdo(teams_df: pd.DataFrame, goalies_df: pd.DataFrame) -> pd.DataFrame:
    """
        adds pdo as a stat to the teams_df. will also add evenStrengthGoals, evenStrengthShootingPercent
        and evenStrengthShots.
        ...

        Parameters
        ----------
        teams_df: pd.DataFrame
            dataframe containing team stats

        goalies_df: pd.DataFrame
            dataframe containing goalies stats

        Returns
        -------
        teams_df: pd.DataFrame
            teams_df with added stats
        """
    # get game ids
    game_ids = teams_df['game_id'].to_list()
    game_ids = helpers.remove_duplicates(game_ids)

    pdos = []

    for id in game_ids:
        goalies_filtered_df = goalies_df[goalies_df['game_id'] == id]

        # Filter to the home team goalies that played in that game
        home_goalies_filtered_df = goalies_filtered_df[goalies_filtered_df['is_home_team'] == True]

        # Filter to the away team goalies that played in that game
        away_goalies_filtered_df = goalies_filtered_df[goalies_filtered_df['is_home_team'] == False]

        # Away shots are taken from the home goalie stats and vice versa
        away_es_shots = home_goalies_filtered_df['evenShotsAgainst'].sum()
        home_es_shots = away_goalies_filtered_df['evenShotsAgainst'].sum()

        away_es_goals = home_goalies_filtered_df['evenShotsAgainst'].sum() - home_goalies_filtered_df['evenSaves'].sum()
        home_es_goals = away_goalies_filtered_df['evenShotsAgainst'].sum() - away_goalies_filtered_df['evenSaves'].sum()

        # Calculate ES Sh%
        home_es_sh_percent = home_es_goals / home_es_shots
        away_es_sh_percent = away_es_goals / away_es_shots

        # Calculate ES Sv%
        home_es_sv_percent = (away_es_shots - away_es_goals) / away_es_shots
        away_es_sv_percent = (home_es_shots - home_es_goals) / home_es_shots

        # Calculate PDO
        home_PDO = home_es_sh_percent + home_es_sv_percent
        away_PDO = away_es_sh_percent + away_es_sv_percent

        # Create dictionary 1 entry for each team
        pdo_dict = [{'game_id': id, 'pdo': home_PDO, 'evenStrengthGoals' : home_es_goals, 'evenStrengthShots' : home_es_shots, 'evenStrengthShootingPercent' : home_es_sh_percent, 'is_home_team' : True},
                    {'game_id': id, 'pdo': away_PDO, 'evenStrengthGoals' : away_es_goals, 'evenStrengthShots' : away_es_shots, 'evenStrengthShootingPercent' : away_es_sh_percent, 'is_home_team' : False}]
        # Append to list
        pdos += pdo_dict

    # Create PDO dataframe
    pdo_df = pd.DataFrame(pdos)

    # Merge PDO's into teams_df
    teams_df = pd.merge(teams_df, pdo_df, left_on=['game_id', 'is_home_team'],
                          right_on=['game_id', 'is_home_team'], how='left')

    return teams_df

def add_sh_per(teams_df: pd.DataFrame) -> pd.DataFrame:
    """
    adds shooting percentage as a stat to the teams_df
    ...

    Parameters
    ----------
    teams_df: pd.DataFrame
        dataframe containing team stats

    Returns
    -------
    teams_df: pd.DataFrame
        teams_df with shooting percentage added
    """
    teams_df['Shooting_Percent'] = teams_df['goals']/teams_df['shots']

    return teams_df

def add_rolling(period, df, stat_columns, is_goalie=False):
    """
    creates rolling average stats in in dataframe provided
    ...

    Parameters
    ----------
    period: int
        the period for which we want to create rolling average
    df: pd.DataFrame
        dataframe to process
    stat_columns: List['str']
        list of columns in dataframe to create rolling stats for

    Returns
    -------
    df: pd.DataFrame
        dataframe with rolling stats added
    """
    for s in stat_columns:
        if 'object' in str(df[s].dtype): continue
        df[s+'_'+str(period)+'_avg'] = df.groupby('team')[s].apply(lambda x:x.rolling(period).mean())
        df[s+'_'+str(period)+'_std'] = df.groupby('team')[s].apply(lambda x:x.rolling(period).std())
        df[s+'_'+str(period)+'_skew'] = df.groupby('team')[s].apply(lambda x:x.rolling(period).skew())

    return df

def get_diff_df(df, name, is_goalie=False):
    """
    calculated stat differentials between home and away team
    ...

    Parameters
    ----------
    df: pd.DataFrame
        dataframe to process
    is_goalie: bool
        if this is a goalie dataframe stats will be grouped by goalies instead of team

    Returns
    -------
    diff_df: pd.DataFrame
        dataframe with calculated stat differentials
    """
    # Sort by date
    df = df.sort_values(by='date').copy()
    newindex = df.groupby('date')['date'].apply(lambda x: x + np.arange(x.size).astype(np.timedelta64))
    df = df.set_index(newindex).sort_index()

    # get stat columns
    stat_cols = [x for x in df.columns if 'int' in str(df[x].dtype)]
    stat_cols.extend([x for x in df.columns if 'float' in str(df[x].dtype)])

    #add rolling stats to the data frame
    df = add_rolling(3, df, stat_cols)
    df = add_rolling(7, df, stat_cols)
    df = add_rolling(14, df, stat_cols)
    df = add_rolling(41, df, stat_cols)
    df = add_rolling(82, df, stat_cols)

    # reset stat columns to just the sma features (removing the original stats)
    df.drop(columns=stat_cols, inplace=True)
    stat_cols = [x for x in df.columns if 'int' in str(df[x].dtype)]
    stat_cols.extend([x for x in df.columns if 'float' in str(df[x].dtype)])

    # shift results so that each row is a pregame stat
    df = df.reset_index(drop=True)
    df = df.sort_values(by='date')

    for s in stat_cols:
        if is_goalie:
            df[s] = df.groupby('goalie_id')[s].shift(1)
        else:
            df[s] = df.groupby('team')[s].shift(1)

    # calculate differences in pregame stats from home vs. away teams
    away_df = df[~df['is_home_team']].copy()
    away_df = away_df.set_index('game_id')
    away_df = away_df[stat_cols]

    home_df = df[df['is_home_team']].copy()
    home_df = home_df.set_index('game_id')
    home_df = home_df[stat_cols]

    diff_df = home_df.subtract(away_df, fill_value=0)
    diff_df = diff_df.reset_index()

    # clean column names
    for s in stat_cols:
        diff_df[name + "_" + s] = diff_df[s]
        diff_df.drop(columns=s, inplace=True)

    return diff_df

def impute_skew(df):
    """
    will impute NaN skew values with 0
    ...

    Parameters
    ----------
    df: pd.DataFrame
        dataframe to process

    Returns
    -------
    df: pd.DataFrame
        dataframe with skew imputed
    """
    cols = list(df.columns.values)

    for c in cols:
        if 'Skew' in c:
            df[c].fillna(0, inplace=True)
        else:
            continue

    return df

def goalie_rest(goalies_df: pd.DataFrame, games_df: pd.DataFrame) -> pd.DataFrame:
    """
    calculates how many rest days a goalie has had with a maximum value of 30 days
    ...

    Parameters
    ----------
    goalies_df: pd.DataFrame
        goalies dataframe
    games_df: pd.Dataframe
        games dataframe

    Returns
    -------
    games_df: pd.DataFrame
        dataframe with goalie rest added
    """
    # It's easier with the way the goalie df is setup to calculate this in here than merge into the main dataframe
    goalies_df['goalie_rest'] = goalies_df.groupby('goalie_id')['date'].diff().dt.days

    # The first teams games in the DF are NaN as there are no previous reference points.
    # We will fill these in with the max value of 30 days as these games were at the start of the season
    goalies_df['goalie_rest'].fillna(30, inplace=True)

    # If the days rest is over 30 just make it 30
    goalies_df.loc[goalies_df["goalie_rest"] > 30, "goalie_rest"] = 30

    # Make a dataframe just containing goalie rest data
    goalie_rest = goalies_df[['game_id', 'goalie_id', 'goalie_rest']]
    # Rename to Home and Away Goalie Rest
    home_goalie_rest = goalie_rest.rename({'goalie_rest': 'home_goalie_rest'}, axis=1)
    away_goalie_rest = goalie_rest.rename({'goalie_rest': 'away_goalie_rest'}, axis=1)

    # Merge into main dataframe
    games_df = pd.merge(games_df, home_goalie_rest, left_on=['game_id', 'home_goalie_id'],
                            right_on=['game_id', 'goalie_id'], how='left')

    games_df = pd.merge(games_df, away_goalie_rest, left_on=['game_id', 'away_goalie_id'],
                            right_on=['game_id', 'goalie_id'], how='left')

    # Remove some columns
    games_df.drop(['goalie_id_x','goalie_id_y'], axis=1, inplace=True)

    return games_df

def team_rest(goalies_df: pd.DataFrame, games_df: pd.DataFrame) -> pd.DataFrame:
    """
    calculates how many rest days a teams has had with a maximum value of 7 days
    ...

    Parameters
    ----------
    goalies_df: pd.DataFrame
        goalies dataframe
    games_df: pd.Dataframe
        games dataframe

    Returns
    -------
    games_df: pd.DataFrame
        dataframe with team rest added
    """
    # TEAM DAYS REST
    # It's easier with the way the goalie df is setup to calculate this in here than merge into the main dataframe
    # Convert date to datetime in goalie dataframe
    goalies_df['team_rest'] = goalies_df.groupby('team')['date'].diff().dt.days
    # The first teams games in the DF are NaN as there are no previous reference points.
    # We will fill these in with the max value of 7 days as these games were at the start of the 2010 season
    goalies_df['team_rest'].fillna(7, inplace=True)
    # If the days rest is over 7 just make it 7
    goalies_df.loc[goalies_df["team_rest"] > 7, "team_rest"] = 7
    # Make a dataframe just containing team rest data
    team_rest = goalies_df[['game_id', 'goalie_id', 'team_rest']]

    # Rename to Home/Away Team Rest
    home_team_rest = team_rest.rename({'team_rest': 'home_team_rest'}, axis=1)
    away_team_rest = team_rest.rename({'team_rest': 'away_team_rest'}, axis=1)

    # Convert data to same types
    home_team_rest['game_id'] = home_team_rest['game_id'].astype('string')
    home_team_rest['goalie_id'] = home_team_rest['goalie_id'].astype('string')
    away_team_rest['game_id'] = away_team_rest['game_id'].astype('string')
    away_team_rest['goalie_id'] = away_team_rest['goalie_id'].astype('string')

    # Merge into main dataframe
    games_df = pd.merge(games_df, home_team_rest, left_on=['game_id', 'home_goalie_id'],
                            right_on=['game_id', 'goalie_id'], how='left')

    games_df = pd.merge(games_df, away_team_rest, left_on=['game_id', 'away_goalie_id'],
                            right_on=['game_id', 'goalie_id'], how='left')

    # Drop some columns
    games_df.drop(['goalie_id_x', 'goalie_id_y'], axis=1, inplace=True)

    return games_df

def rolling_win_percentage(games_df: pd.DataFrame, period: int) -> pd.DataFrame:
    """
    creates a moving average win percentage
    ...

    Parameters
    ----------
    games_df: pd.DataFrame
        games dataframe
    period: int
        period for which we want to calculate win percentage

    Returns
    -------
    games_df: pd.DataFrame
        dataframe with rolling win percentage added
    """
    # Target Encoding, this will create a period SMA win percentage columns for the home and away teams
    # todo double check this is calculating actual win percent and not just home/away win percent
    column_names = ['home_win_percent_'+str(period)+'_avg','away_win_percent_'+str(period)+'_avg']

    for x in column_names:
        if x == 'home_win_percent_'+str(period)+'_avg':
            games_df[x] = games_df.groupby('home_team')['home_team_win'].apply(lambda x: x.rolling(period).mean()).shift(1)
        else:
            games_df[x] = games_df.groupby('away_team')['home_team_win'].apply(lambda x: x.rolling(period).mean()).shift(1)
    return games_df

if __name__ == '__main__':
    # import games_df
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/games_df.pkl', 'rb') as f:
        games_df = pickle.load(f)

    new_game_ids = update_game_ids(20202021, games_df)

    # retrieve team game by game stats for all new game ids pulled
    new_team_stats = process_data.pull_team_stats(new_game_ids)

    # retrieve goalie game by game stats for all new game ids pulled
    new_goalie_stats = process_data.pull_goalie_stats(new_game_ids)

    # retrieve game by game information for all new game ids pulled
    new_games_info = process_data.pull_game_info(new_game_ids)

    # open old databases
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/team_stats.pkl', 'rb') as f:
        old_team_stats = pickle.load(f)
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/goalie_stats.pkl', 'rb') as f:
        old_goalie_stats = pickle.load(f)
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/games_info.pkl', 'rb') as f:
        old_games_info = pickle.load(f)

    # combine new with old
    team_stats_list = old_team_stats + new_team_stats
    goalie_stats_list = old_goalie_stats + new_goalie_stats
    games_list = old_games_info + new_games_info

    # process data
    # make teams df
    teams_df = make_teams_df(team_stats_list)

    # make goalies df
    goalies_df = make_goalies_df(goalie_stats_list)

    # make games df
    games_df = make_games_df(games_list)

    # convert to numerical
    teams_df, goalies_df = convert_numerical(teams_df, goalies_df)

    # add pdo
    teams_df = add_pdo(teams_df, goalies_df)

    # add shooting percent
    teams_df = add_sh_per(teams_df)

    # remove columns that will not be used in teams_df and goalies_df
    teams_df.drop(['index'], axis=1, inplace=True)
    goalies_df.drop(['index', 'assists', 'goals', 'pim', 'decision'], axis=1, inplace=True)

    # convert ids to strings
    teams_df['game_id'] = teams_df['game_id'].map(str)
    teams_df['goalie_id'] = teams_df['goalie_id'].map(str)
    goalies_df['game_id'] = goalies_df['game_id'].map(str)
    goalies_df['goalie_id'] = goalies_df['goalie_id'].map(str)
    games_df['game_id'] = games_df['game_id'].map(str)
    games_df['home_goalie_id'] = games_df['home_goalie_id'].map(str)
    games_df['away_goalie_id'] = games_df['away_goalie_id'].map(str)

    # create rolling stats in main games dataframe

    games_df = pd.merge(left=games_df, right=get_diff_df(teams_df, 'teams'),
                  on='game_id', how='left')

    print(games_df.shape)

    games_df = pd.merge(left=games_df, right=get_diff_df(goalies_df, 'goalies', is_goalie=True),
                  on='game_id', how='left')

    # drop duplicates due to multiple goalies playing in one game
    # todo confirm if the first or last game should be kept
    games_df.drop_duplicates(subset=['game_id'], keep="last", inplace=True)

    print(games_df.shape)

    # impute skews
    games_df = impute_skew(games_df)

    # add goalie rest
    games_df = goalie_rest(goalies_df, games_df)

    # add team rest
    games_df = team_rest(goalies_df, games_df)

    # add power rankings
    games_df = power_rankings.fast_elo_ratings(games_df)
    games_df = power_rankings.slow_elo_ratings(games_df)
    games_df = power_rankings.glicko(games_df)
    games_df = power_rankings.trueskill(games_df)

    # Add Win Percentage for last 10,20,41 and 82 games
    days = [10, 20, 41, 82]

    for d in days:
        games_df = rolling_win_percentage(games_df, d)

    # Remove rows with NaN due to our SMA calculation
    games_df = games_df.dropna()  # Drop rows with missing values

    games_df.reset_index(inplace=True, drop=True)
    print(games_df)

    # Pickle and games_df for machine learning
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/games_df.pkl', 'wb') as f:
        pickle.dump(games_df, f)

    # update database pickle files
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/team_stats.pkl', 'wb') as f:
        pickle.dump(team_stats_list, f)
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/goalie_stats.pkl', 'wb') as f:
        pickle.dump(goalie_stats_list, f)
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/games_info.pkl', 'wb') as f:
        pickle.dump(team_stats_list, f)
