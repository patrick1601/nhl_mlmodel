# This module defines the functions for calculating the power rankings for our machine learning model
# The following power rankings systems will be used:
#   Slow Changing ELO
#   Fast Changing ELO
#   Glicko
#   TrueSkill

from elote import EloCompetitor
from elote import GlickoCompetitor
import pickle
import pandas as pd
import sys
from trueskill import Rating, quality, rate

# Show full columns on dataframes
pd.set_option('display.expand_frame_repr', False)

def fast_elo_ratings(df):
    """
    creates fast moving elo ratings
    ...

    Parameters
    ----------
    df: pd.DataFrame
        games dataframe

    Returns
    -------
    df: pd.DataFrame
        with fast moving elo ratings added
    """
    ratings = {}
    for x in df.home_team.unique():
        ratings[x] = EloCompetitor()
    for x in df.away_team.unique():
        ratings[x] = EloCompetitor()

    home_team_elo = []
    away_team_elo = []
    elo_exp = []

    df = df.sort_values(by='date').reset_index(drop=True)
    for i, r in df.iterrows():
        # get pre-game ratings
        elo_exp.append(ratings[r.home_team].expected_score(ratings[r.away_team]))
        home_team_elo.append(ratings[r.home_team].rating)
        away_team_elo.append(ratings[r.away_team].rating)

        # update ratings
        if r.home_team_win:
            ratings[r.home_team].beat(ratings[r.away_team])
        else:
            ratings[r.away_team].beat(ratings[r.home_team])

    df['elo_exp'] = elo_exp
    df['home_team_elo'] = home_team_elo
    df['away_team_elo'] = away_team_elo

    return df

def slow_elo_ratings(df):
    """
    creates slow moving elo ratings
    ...

    Parameters
    ----------
    df: pd.DataFrame
        games dataframe

    Returns
    -------
    df: pd.DataFrame
        with slow moving elo ratings added
    """
    ratings = {}

    # Obtain team names
    for x in df.home_team.unique():
        ratings[x]=EloCompetitor()
        ratings[x]._k_factor = 16
    for x in df.away_team.unique():
        ratings[x]=EloCompetitor()
        ratings[x]._k_factor = 16

    home_team_elo = []
    away_team_elo = []
    elo_exp = []

    df = df.sort_values(by='date').reset_index(drop=True)
    for i, r in df.iterrows():
        # get pre-game ratings
        elo_exp.append(ratings[r.home_team].expected_score(ratings[r.away_team]))
        home_team_elo.append(ratings[r.home_team].rating)
        away_team_elo.append(ratings[r.away_team].rating)

        # update ratings
        if r.home_team_win:
            ratings[r.home_team].beat(ratings[r.away_team])
        else:
            ratings[r.away_team].beat(ratings[r.home_team])

    df['slow_elo_exp'] = elo_exp
    df['home_team_slow_elo'] = home_team_elo
    df['away_team_slow_elo'] = away_team_elo

    return df

def glicko(df):
    """
    creates glicko ratings
    ...

    Parameters
    ----------
    df: pd.DataFrame
        games dataframe

    Returns
    -------
    df: pd.DataFrame
        with fast moving glicko added
    """
    ratings = {}
    for x in df.home_team.unique():
        ratings[x] = GlickoCompetitor()
    for x in df.away_team.unique():
        ratings[x] = GlickoCompetitor()

    home_team_glick = []
    away_team_glick = []
    glick_exp = []

    df = df.sort_values(by='date').reset_index(drop=True)
    for i, r in df.iterrows():
        # get pregame ratings
        glick_exp.append(ratings[r.home_team].expected_score(ratings[r.away_team]))
        home_team_glick.append(ratings[r.home_team].rating)
        away_team_glick.append(ratings[r.away_team].rating)
        # update ratings
        if r.home_team_win:
            ratings[r.home_team].beat(ratings[r.away_team])
        else:
            ratings[r.away_team].beat(ratings[r.home_team])

    df['glick_exp'] = glick_exp
    df['home_team_glick'] = home_team_glick
    df['away_team_glick'] = away_team_glick

    return df

def trueskill(df):
    """
    creates trueskill ratings
    ...

    Parameters
    ----------
    df: pd.DataFrame
        games dataframe

    Returns
    -------
    df: pd.DataFrame
        with trueskill ratings added
    """
    ratings = {}
    for x in df.home_team.unique():
        ratings[x] = Rating(25)
    for x in df.away_team.unique():
        ratings[x] = Rating(25)
    for x in df.home_goalie_id.unique():
        ratings[x] = Rating(25)
    for x in df.away_goalie_id.unique():
        ratings[x] = Rating(25)

    ts_quality = []
    goalie_ts_diff = []
    team_ts_diff = []
    home_goalie_ts = []
    away_goalie_ts = []
    home_team_ts = []
    away_team_ts = []
    df = df.sort_values(by='date').copy()

    for i, r in df.iterrows():
        # get pre-match trueskill ratings from dict
        match = [(ratings[r.home_team], ratings[r.home_goalie_id]),
                 (ratings[r.away_team], ratings[r.away_goalie_id])]
        ts_quality.append(quality(match))
        goalie_ts_diff.append(ratings[r.home_goalie_id].mu - ratings[r.away_goalie_id].mu)
        team_ts_diff.append(ratings[r.home_team].mu - ratings[r.away_team].mu)
        home_goalie_ts.append(ratings[r.home_goalie_id].mu)
        away_goalie_ts.append(ratings[r.away_goalie_id].mu)
        home_team_ts.append(ratings[r.home_team].mu)
        away_team_ts.append(ratings[r.away_team].mu)

        if r.date < df.date.max():
            # update ratings dictionary with post-match ratings
            if r.home_team_win:
                match = [(ratings[r.home_team], ratings[r.home_goalie_id]),
                         (ratings[r.away_team], ratings[r.away_goalie_id])]
                [(ratings[r.home_team], ratings[r.home_goalie_id]),
                 (ratings[r.away_team], ratings[r.away_goalie_id])] = rate(match)
            else:
                match = [(ratings[r.away_team], ratings[r.away_goalie_id]),
                         (ratings[r.home_team], ratings[r.home_goalie_id])]
                [(ratings[r.away_team], ratings[r.away_goalie_id]),
                 (ratings[r.home_team], ratings[r.home_goalie_id])] = rate(match)

    df['ts_game_quality'] = ts_quality
    df['goalie_ts_diff'] = goalie_ts_diff
    df['team_ts_diff'] = team_ts_diff
    df['home_goalie_ts'] = home_goalie_ts
    df['away_goalie_ts'] = away_goalie_ts
    df['home_team_ts'] = home_team_ts
    df['away_team_ts'] = away_team_ts

    return df

if __name__ == '__main__':
    pass