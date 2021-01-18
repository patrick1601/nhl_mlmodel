# This module defines the functions to scrape the NHL.com API
from bs4 import BeautifulSoup
import datetime as dt
import json
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import List

class NhlTeam:
    """
        represents a game played in the nhl by 1 team

        ...

        Parameters
        ----------
        date: dt.datetime
            date on which the game was played
        game_id: int
            game id to identify the game
        team: str
            team abbreviation
        is_home_team: boolean
            True if team is home
        home_team_win: boolean
            True if the home team won
        goals: int
            goals scored
        pim: int
            pims
        shots: int
            shots
        powerPlayPercentage: float
            power play percentage
        powerPlayGoals: int
            power play goals
        powerPlayOpportunities: int
            powerPlayOpportunities
        faceOffWinPercentage: float
            face off win percentage
        blocked: int
            blocked shots
        takeaways: int
            takeaways
        giveaways: int
            giveaways
        hits: int
            hits
        goalie_id: int
            goalie id
        goalie_name: str
            goalie name
        """

    def __init__(self, date: dt.datetime, game_id: int, team: str, is_home_team: bool, home_team_win: bool,
                 goals: int, pim: int, shots: int, powerPlayPercentage: float, powerPlayGoals: int,
                 powerPlayOpportunities: int, faceOffWinPercentage: float, blocked: int, takeaways: int,
                 giveaways: int, hits: int, goalie_id: int, goalie_name: str):
        self.date = date
        self.game_id = game_id
        self.team = team
        self.is_home_team = is_home_team
        self.home_team_win = home_team_win
        self.goals = goals
        self.pim = pim
        self.shots = shots
        self.powerPlayPercentage = powerPlayPercentage
        self.powerPlayGoals = powerPlayGoals
        self.powerPlayOpportunities = powerPlayOpportunities
        self.faceOffWinPercentage = faceOffWinPercentage
        self.blocked = blocked
        self.takeaways = takeaways
        self.giveaways = giveaways
        self.hits = hits
        self.goalie_id = goalie_id
        self.goalie_name = goalie_name

    def to_dict(self):
        return {
            'date': self.date,
            'game_id': self.game_id,
            'team': self.team,
            'is_home_team': self.is_home_team,
            'home_team_win': self.home_team_win,
            'goals': self.goals,
            'pim': self.pim,
            'shots': self.shots,
            'powerPlayPercentage': self.powerPlayPercentage,
            'powerPlayGoals': self.powerPlayGoals,
            'powerPlayOpportunities': self.powerPlayOpportunities,
            'faceOffWinPercentage': self.faceOffWinPercentage,
            'blocked': self.blocked,
            'takeaways': self.takeaways,
            'giveaways': self.giveaways,
            'hits': self.hits,
            'goalie_id': self.goalie_id,
            'goalie_name': self.goalie_name
        }

class NhlGoalie:
    """
        represents a game played in the nhl by 1 goalie

        ...

        Parameters
        ----------
        date: dt.datetime
            date on which the game was played
        game_id: int
            game id to identify the game
        team: str
            team abbreviation
        is_home_team: boolean
            True if team is home
        goalie_name: str
            goalie name
        goalie_id: int
            goalie id
        timeOnIce: str
            time on ice for the goalie
        assists: int
            assists
        goals: int
            goals
        pim: int
            penalties in minutes
        shots: int
            shots faced
        saves: int
            saves
        powerPlaySaves: int
            saves while goalies team was on the penalty kill
        shortHandedSaves: int
            saves while goalies team was on the powerplay
        evenSaves: int
            evenstrength saves
        shortHandedShotsAgainst: int
            shots faced while goalies team was on the powerplay
        evenShotsAgainst: int
            shots faced during evenstrength
        powerPlayShotsAgainst: int
            shots faces while goalies team was on the penalty kill
        decision: str
            W if goalie won L if goalie lost
        savePercentage: float
            goalie save percentage
        evenStrengthSavePercentage: float
            save percentage during even strength
        """

    def __init__(self, date: dt.datetime, game_id: int, team: str, is_home_team: bool, goalie_name: str,
                 goalie_id: int, timeOnIce: str, assists: int, goals: int, pim: int, shots: int,
                 saves: int, powerPlaySaves: int, shortHandedSaves: int, evenSaves: int,
                 shortHandedShotsAgainst: int, evenShotsAgainst: int, powerPlayShotsAgainst: int,
                 decision: str, savePercentage: float, evenStrengthSavePercentage: float):
        self.date = date
        self.game_id = game_id
        self.team = team
        self.is_home_team = is_home_team
        self.goalie_id = goalie_id
        self.goalie_name = goalie_name
        self.timeOnIce = timeOnIce
        self.assists = assists
        self.goals = goals
        self.pim = pim
        self.shots = shots
        self.saves = saves
        self.powerPlaySaves = powerPlaySaves
        self.shortHandedSaves = shortHandedSaves
        self.evenSaves = evenSaves
        self.shortHandedShotsAgainst = shortHandedShotsAgainst
        self.evenShotsAgainst = evenShotsAgainst
        self.powerPlayShotsAgainst = powerPlayShotsAgainst
        self.decision = decision
        self.savePercentage = savePercentage
        self.evenStrengthSavePercentage = evenStrengthSavePercentage

    def to_dict(self):
        return {
            'date': self.date,
            'game_id': self.game_id,
            'team': self.team,
            'is_home_team': self.is_home_team,
            'goalie_id': self.goalie_id,
            'goalie_name': self.goalie_name,
            'timeOnIce': self.timeOnIce,
            'assists': self.assists,
            'goals': self.goals,
            'pim': self.pim,
            'shots': self.shots,
            'saves': self.saves,
            'powerPlaySaves': self.powerPlaySaves,
            'shortHandedSaves': self.shortHandedSaves,
            'evenSaves': self.evenSaves,
            'shortHandedShotsAgainst': self.shortHandedShotsAgainst,
            'evenShotsAgainst': self.evenShotsAgainst,
            'powerPlayShotsAgainst': self.powerPlayShotsAgainst,
            'decision': self.decision,
            'savePercentage': self.savePercentage,
            'evenStrengthSavePercentage': self.evenStrengthSavePercentage
        }

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
            team abbreviation
        away_team: str
            team abbreviation
        home_team_win: boolean
            True if the home team won
        home_goalie_id: int
            home goalie id
        away_goalie_id: int
            away goalie id
        home_goalie_name: str
            home goalie name
        away_goalie_name:str
            away goalie name
        """
    def __init__(self, date: dt.datetime, game_id: int, home_team: str, away_team: str,
                 home_team_win: bool, home_goalie_id: int, away_goalie_id: int, home_goalie_name:str,
                 away_goalie_name:str):
        self.date = date
        self.game_id = game_id
        self.home_team = home_team
        self.away_team = away_team
        self.home_team_win = home_team_win
        self.home_goalie_id = home_goalie_id
        self.away_goalie_id = away_goalie_id
        self.home_goalie_name = home_goalie_name
        self.away_goalie_name = away_goalie_name

def get_game_ids(season: int) -> List[int]:
    """
    retrieves all of the gameids for the specified season

    ...

    Parameters
    ----------
    season: int
        should be entered as an integer in the following format: 20192020

    Returns
    -------
    game_ids: List[int]
        list of game ids for the specified season
    """
    season_str: str = str(season)
    url: str = f"https://statsapi.web.nhl.com/api/v1/schedule?season={season_str}&gameType=R"
    resp = requests.get(url)
    raw_schedule = json.loads(resp.text)
    schedule = raw_schedule['dates']
    # Each entry in schedule is a day in the NHL. Each 'games' key contains all the games on that day.
    # Therefore we need a nested loop to retrieve all games

    game_ids=[]

    for day in schedule:
        # Retrieve list that shows all games played on that day
        games = day['games']
        # Loop through games and retrieve ids
        for game in games:
            game_id = game['gamePk']
            game_ids.append(game_id)
    return game_ids

def scrape_team_stats(game_id: int) -> List[NhlTeam]:
    """
        returns two entries in a List. The first entry is for the home team and the second is the away team.
        Each entry represents 1 game played.

        Refer to: https://github.com/dword4/nhlapi on how to use the NHL API

        ...

        Parameters
        ----------
        game_id: int
            game id we are retrieving data for

        Returns
        -------
        teams: List[NhlTeam]
            list containing an entry for the home team and away team playing in the same game
        """

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    url = f'https://statsapi.web.nhl.com/api/v1/game/{str(game_id)}/feed/live'
    resp = session.get(url)
    json_data = json.loads(resp.text)

    # RETRIEVE STATS REQUIRED

    # retrieve date and convert to date time
    game_date: str = json_data['gameData']['datetime']['dateTime']
    game_date = dt.datetime.strptime(game_date, '%Y-%m-%dT%H:%M:%SZ')

    # Retrieve team names
    home_team: str = json_data["liveData"]['boxscore']['teams']['home']['team']['abbreviation']
    away_team: str = json_data["liveData"]['boxscore']['teams']['away']['team']['abbreviation']

    # Collect list of teamSkaterStats we want to retrieve from json data
    team_skater_stats_home = json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']
    team_skater_stats_away = json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']

    # Starting goalies
    # spot checked a few APIs and it seems like the starting goalie will be listed last in the json
    # file if he was pulled. The goalie that finishes the game will be listed first (0).
    home_team_starting_goalie_id = json_data["liveData"]['boxscore']['teams']['home']['goalies'][-1]
    away_team_starting_goalie_id = json_data["liveData"]['boxscore']['teams']['away']['goalies'][-1]
    home_team_starting_goalie_name = json_data["liveData"]['boxscore']['teams']['home']['players']['ID'+str(home_team_starting_goalie_id)]['person']['fullName']
    away_team_starting_goalie_name = json_data["liveData"]['boxscore']['teams']['away']['players']['ID'+str(away_team_starting_goalie_id)]['person']['fullName']

    # retrieve outcome (same for both home team and away team)
    if json_data['liveData']['linescore']['hasShootout']==False:
        if json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']['goals'] > json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']['goals']:
            home_team_win = True
        if json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']['goals'] < json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']['goals']:
            home_team_win = False
    if json_data['liveData']['linescore']['hasShootout']==True:
        if json_data['liveData']['linescore']['shootoutInfo']['home']['scores'] > json_data['liveData']['linescore']['shootoutInfo']['away']['scores']:
            home_team_win = True
        if json_data['liveData']['linescore']['shootoutInfo']['home']['scores'] < json_data['liveData']['linescore']['shootoutInfo']['away']['scores']:
            home_team_win = False

    # create NhlTeam objects for the home and away team
    home_team_stats = NhlTeam(date=game_date, game_id=game_id,
                              team=home_team, is_home_team=True, home_team_win=home_team_win,
                              goals=team_skater_stats_home['goals'],
                              pim=team_skater_stats_home['pim'], shots=team_skater_stats_home['shots'],
                              powerPlayPercentage=team_skater_stats_home['powerPlayPercentage'],
                              powerPlayGoals=team_skater_stats_home['powerPlayGoals'],
                              powerPlayOpportunities=team_skater_stats_home['powerPlayOpportunities'],
                              faceOffWinPercentage=team_skater_stats_home['faceOffWinPercentage'],
                              blocked=team_skater_stats_home['blocked'],
                              takeaways=team_skater_stats_home['takeaways'],
                              giveaways=team_skater_stats_home['giveaways'],
                              hits=team_skater_stats_home['hits'], goalie_id=home_team_starting_goalie_id,
                              goalie_name=home_team_starting_goalie_name)

    away_team_stats = NhlTeam(date=game_date, game_id=game_id,
                              team=away_team, is_home_team=False, home_team_win=home_team_win,
                              goals=team_skater_stats_away['goals'],
                              pim=team_skater_stats_away['pim'], shots=team_skater_stats_away['shots'],
                              powerPlayPercentage=team_skater_stats_away['powerPlayPercentage'],
                              powerPlayGoals=team_skater_stats_away['powerPlayGoals'],
                              powerPlayOpportunities=team_skater_stats_away['powerPlayOpportunities'],
                              faceOffWinPercentage=team_skater_stats_away['faceOffWinPercentage'],
                              blocked=team_skater_stats_away['blocked'],
                              takeaways=team_skater_stats_away['takeaways'],
                              giveaways=team_skater_stats_away['giveaways'],
                              hits=team_skater_stats_away['hits'], goalie_id=away_team_starting_goalie_id,
                              goalie_name=away_team_starting_goalie_name)

    teams = [home_team_stats, away_team_stats]

    return teams

def scrape_goalie_stats(game_id: int) -> List[NhlGoalie]:
    """
        retrieves a list of NhlGoalie containing goalie stats for all goalies that played in the game
        specified by game_id

        Refer to: https://github.com/dword4/nhlapi on how to use the NHL API

        ...

        Parameters
        ----------
        game_id: int
            game id we are retrieving data for

        Returns
        -------
        team_stats: List[NhlTeam]
            list containing an entry for the home team and away team playing in the same game
        """
    # backoff strategy to avoid maxretry errors
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    url = f'https://statsapi.web.nhl.com/api/v1/game/{str(game_id)}/feed/live'
    resp = session.get(url)
    json_data = json.loads(resp.text)

    # RETRIEVE STATS REQUIRED

    # get date
    game_date = json_data['gameData']['datetime']['dateTime']
    game_date = dt.datetime.strptime(game_date, '%Y-%m-%dT%H:%M:%SZ')

    # Get goalie team
    home_goalie_team = json_data['gameData']['teams']['home']['abbreviation']
    away_goalie_team = json_data['gameData']['teams']['away']['abbreviation']

    # Get goalie IDs
    home_goalie_id = json_data['liveData']['boxscore']['teams']['home']['goalies']
    away_goalie_id = json_data['liveData']['boxscore']['teams']['away']['goalies']

    # Get goalie names
    home_goalie_names = []
    away_goalie_names = []

    for i in home_goalie_id: # for loop to iterate through list of home goalies that played this game
        j = json_data['liveData']['boxscore']['teams']['home']['players']['ID' + str(i)]['person']['fullName']
        home_goalie_names.append(j)
    for i in away_goalie_id: # for loop to iterate through list of away goalies that played this game
        j = json_data['liveData']['boxscore']['teams']['away']['players']['ID' + str(i)]['person']['fullName']
        away_goalie_names.append(j)

    # Get goalie stats
    home_goalie_stats = []
    away_goalie_stats = []
    for i in home_goalie_id: # for loop to iterate through list of home goalies that played this game
        j = json_data['liveData']['boxscore']['teams']['home']['players']['ID' + str(i)]['stats']['goalieStats']
        home_goalie_stats.append(j)
    for i in away_goalie_id: # for loop to iterate through list of home goalies that played this game
        j = json_data['liveData']['boxscore']['teams']['away']['players']['ID' + str(i)]['stats']['goalieStats']
        away_goalie_stats.append(j)

    # make home goalie list. for loop needed as there could be more than 2 goalies playing in 1 game
    home_goalies = []
    counter = list(range(len(home_goalie_stats))) # counter for number of goalies that played

    for g in counter:
        try:
            toi = home_goalie_stats[g]['timeOnIce']
        except KeyError:
            toi = None
        try:
            a = home_goalie_stats[g]['assists']
        except KeyError:
            a = None
        try:
            goal = home_goalie_stats[g]['goals']
        except KeyError:
            goal = None
        try:
            pims = home_goalie_stats[g]['pim']
        except KeyError:
            pims = None
        try:
            s = home_goalie_stats[g]['shots']
        except KeyError:
            s = None
        try:
            sv = home_goalie_stats[g]['saves']
        except KeyError:
            sv = None
        try:
            ppsv = home_goalie_stats[g]['powerPlaySaves']
        except KeyError:
            ppsv = None
        try:
            shsv = home_goalie_stats[g]['shortHandedSaves']
        except KeyError:
            shsv = None
        try:
            esv = home_goalie_stats[g]['evenSaves']
        except KeyError:
            esv = None
        try:
            shsa = home_goalie_stats[g]['shortHandedShotsAgainst']
        except KeyError:
            shsa = None
        try:
            esa = home_goalie_stats[g]['evenShotsAgainst']
        except KeyError:
            esa = None
        try:
            ppsa = home_goalie_stats[g]['powerPlayShotsAgainst']
        except KeyError:
            ppsa = None
        try:
            dec = home_goalie_stats[g]['decision']
        except KeyError:
            dec = None
        try:
            svper = home_goalie_stats[g]['savePercentage']
        except KeyError:
            svper = None
        try:
            essvper = home_goalie_stats[g]['evenStrengthSavePercentage']
        except KeyError:
            essvper = None




        goalie_stats = NhlGoalie(date=game_date, game_id=game_id, team=home_goalie_team, is_home_team=True,
                                 goalie_name=home_goalie_names[g], goalie_id=home_goalie_id[g], timeOnIce=toi,
                                 assists=a, goals=goal, pim=pims, shots=s, saves=sv, powerPlaySaves=ppsv,
                                 shortHandedSaves=shsv, evenSaves=esv, shortHandedShotsAgainst=shsa,
                                 evenShotsAgainst=esa, powerPlayShotsAgainst=ppsa, decision=dec,
                                 savePercentage= svper, evenStrengthSavePercentage=essvper)
        home_goalies.append(goalie_stats)

    # make away goalie list. for loop needed as there could be more than 2 goalies playing in 1 game
    away_goalies = []
    counter = list(range(len(away_goalie_stats))) # counter for number of goalies that played

    for g in counter:
        try:
            toi = away_goalie_stats[g]['timeOnIce']
        except KeyError:
            toi = None
        try:
            a = away_goalie_stats[g]['assists']
        except KeyError:
            a = None
        try:
            goal = away_goalie_stats[g]['goals']
        except KeyError:
            goal = None
        try:
            pims = away_goalie_stats[g]['pim']
        except KeyError:
            pims = None
        try:
            s = away_goalie_stats[g]['shots']
        except KeyError:
            s = None
        try:
            sv = away_goalie_stats[g]['saves']
        except KeyError:
            sv = None
        try:
            ppsv = away_goalie_stats[g]['powerPlaySaves']
        except KeyError:
            ppsv = None
        try:
            shsv = away_goalie_stats[g]['shortHandedSaves']
        except KeyError:
            shsv = None
        try:
            esv = away_goalie_stats[g]['evenSaves']
        except KeyError:
            esv = None
        try:
            shsa = away_goalie_stats[g]['shortHandedShotsAgainst']
        except KeyError:
            shsa = None
        try:
            esa = away_goalie_stats[g]['evenShotsAgainst']
        except KeyError:
            esa = None
        try:
            ppsa = away_goalie_stats[g]['powerPlayShotsAgainst']
        except KeyError:
            ppsa = None
        try:
            dec = away_goalie_stats[g]['decision']
        except KeyError:
            dec = None
        try:
            svper = away_goalie_stats[g]['savePercentage']
        except KeyError:
            svper = None
        try:
            essvper = away_goalie_stats[g]['evenStrengthSavePercentage']
        except KeyError:
            essvper = None

        goalie_stats = NhlGoalie(date=game_date, game_id=game_id, team=away_goalie_team, is_home_team=False,
                                 goalie_name=away_goalie_names[g], goalie_id=away_goalie_id[g], timeOnIce=toi,
                                 assists=a, goals=goal, pim=pims, shots=s, saves=sv, powerPlaySaves=ppsv,
                                 shortHandedSaves=shsv, evenSaves=esv, shortHandedShotsAgainst=shsa,
                                 evenShotsAgainst=esa, powerPlayShotsAgainst=ppsa, decision=dec,
                                 savePercentage=svper, evenStrengthSavePercentage=essvper)
        away_goalies.append(goalie_stats)

    # Merge the two lists
    goalie_stats = away_goalies + home_goalies

    return goalie_stats

def scrape_game_info(game_id:int) -> NhlGame:
    """
        returns an NhlGame object with parameters for the game_id provided

        Refer to: https://github.com/dword4/nhlapi on how to use the NHL API

        ...

        Parameters
        ----------
        game_id: int
            game id we are retrieving data for

        Returns
        -------
        game: NhlGame
            NhlGame object with info for the game_id provided
        """
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    url = f'https://statsapi.web.nhl.com/api/v1/game/{str(game_id)}/feed/live'
    resp = session.get(url)
    json_data = json.loads(resp.text)

    # RETRIEVE INFO REQUIRED

    # retrieve date and convert to date time
    game_date: str = json_data['gameData']['datetime']['dateTime']
    game_date = dt.datetime.strptime(game_date, '%Y-%m-%dT%H:%M:%SZ')

    # Retrieve team names
    home_team: str = json_data["liveData"]['boxscore']['teams']['home']['team']['abbreviation']
    away_team: str = json_data["liveData"]['boxscore']['teams']['away']['team']['abbreviation']

    # retrieve outcome
    if json_data['liveData']['linescore']['hasShootout']==False:
        if json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']['goals'] > json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']['goals']:
            home_team_win = True
        if json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']['goals'] < json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']['goals']:
            home_team_win = False
    if json_data['liveData']['linescore']['hasShootout']==True:
        if json_data['liveData']['linescore']['shootoutInfo']['home']['scores'] > json_data['liveData']['linescore']['shootoutInfo']['away']['scores']:
            home_team_win = True
        if json_data['liveData']['linescore']['shootoutInfo']['home']['scores'] < json_data['liveData']['linescore']['shootoutInfo']['away']['scores']:
            home_team_win = False

    # Starting goalies
    # spot checked a few APIs and it seems like the starting goalie will be listed last in the json
    # file if he was pulled. The goalie that finishes the game will be listed first (0).
    home_team_starting_goalie_id = json_data["liveData"]['boxscore']['teams']['home']['goalies'][-1]
    away_team_starting_goalie_id = json_data["liveData"]['boxscore']['teams']['away']['goalies'][-1]
    home_team_starting_goalie_name = \
    json_data["liveData"]['boxscore']['teams']['home']['players']['ID' + str(home_team_starting_goalie_id)][
        'person']['fullName']
    away_team_starting_goalie_name = \
    json_data["liveData"]['boxscore']['teams']['away']['players']['ID' + str(away_team_starting_goalie_id)][
        'person']['fullName']

    game_info = NhlGame(date=game_date, game_id=game_id, home_team=home_team, away_team=away_team,
                        home_team_win=home_team_win, home_goalie_id=home_team_starting_goalie_id,
                        away_goalie_id=away_team_starting_goalie_id,
                        home_goalie_name=home_team_starting_goalie_name,
                        away_goalie_name=away_team_starting_goalie_name)
    return game_info


def retrieve_team(game_id: int, home: bool) -> str:
    """
    retrieves the team abbreviation playing in an NHL game

    ...

    Parameters
    ----------
    game_id: int
        game id we are retrieving data for
    home: bool
        if True retrieves the home team, False retrieves away

    Returns
    -------
    team: str
        team abbreviation
    """

    url = f'https://statsapi.web.nhl.com/api/v1/game/{str(game_id)}/feed/live'
    resp = requests.get(url)
    json_data = json.loads(resp.text)

    if home:
        team = json_data['gameData']['teams']['home']['abbreviation']
    else:
        team = json_data['gameData']['teams']['away']['abbreviation']

    return team

def retrieve_date(game_id: int) -> dt.datetime:
    """
    retrieves the date an NHL game was played

    ...

    Parameters
    ----------
    game_id: int
        game id we are retrieving data for

    Returns
    -------
    date: dt.datetime
        date that NHL game was played
    """
    url = f'https://statsapi.web.nhl.com/api/v1/game/{str(game_id)}/feed/live'
    resp = requests.get(url)
    json_data = json.loads(resp.text)

    date = json_data['gameData']['datetime']['dateTime']
    date = dt.datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')

    return date

def get_starting_goalies(home_abv: str, away_abv: str, date: str) -> (str, str):
    """
    scrapes starting goaltenders from dailyfaceoff.com for the specified date and teams

    ...

    Parameters
    ----------
    home_abv: str
        abbreviation for home team
    away_abv: str
        abbreviation for away team
    date: str
        string for which we want to retrieve starting goalies (ex. '01-13-2021')

    Returns
    -------
    home_goalie: str
        home goalie name
    away_goalie: str
        away goalie name
    """

    # First define a dictionary to translate team abbreviations in our df to the team names used on daily
    # faceoff

    team_translations = {'MIN':'Minnesota Wild','TOR':'Toronto Maple Leafs',
                         'PIT':'Pittsburgh Penguins', 'COL':'Colorado Avalanche',
                         'EDM':'Edmonton Oilers', 'CAR':'Carolina Hurricanes',
                         'CBJ':'Columbus Blue Jackets', 'NJD':'New Jersey Devils',
                         'DET':'Detroit Red Wings', 'OTT':'Ottawa Senators',
                         'BOS':'Boston Bruins', 'SJS':'San Jose Sharks',
                         'BUF':'Buffalo Sabres','NYI':'New York Islanders',
                         'WSH':'Washington Capitals','TBL':'Tampa Bay Lightning',
                         'STL':'St Louis Blues', 'NSH':'Nashville Predators',
                         'CHI':'Chicago Blackhawks', 'VAN':'Vancouver Canucks',
                         'CGY':'Calgary Flames', 'PHI':'Philadelphia Flyers',
                         'LAK':'Los Angeles Kings', 'MTL':'Montreal Canadiens',
                         'ANA':'Anaheim Ducks', 'DAL':'Dallas Stars',
                         'NYR':'New York Rangers', 'FLA':'Florida Panthers',
                         'WPG':'Winnipeg Jets', 'ARI':'Arizona Coyotes',
                         'VGK':'Vegas Golden Knights'}

    home_team = team_translations[home_abv]
    away_team = team_translations[away_abv]

    url = f'https://www.dailyfaceoff.com/starting-goalies/{date}'

    # Need headers as daily faceoff will block the get request without one
    headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'}
    result = requests.get(url, headers=headers)

    # Parse the data
    src = result.content
    soup = BeautifulSoup(src, 'lxml')

    goalie_boxes = soup.find_all('div', {'class':'starting-goalies-card stat-card'})

    # find the goalie box that contains the games we are looking for
    for count, box in enumerate(goalie_boxes):
        if home_team and away_team in box.text:
            goalie_box = goalie_boxes[count]
        else:
            continue
    # retrieve the h4 headings which contain the starting goalies
    h4 = goalie_box.find_all('h4')
    # Away goalie is at element 1 and home goalie is at element 2
    away_goalie = h4[1].text
    home_goalie = h4[2].text

    return home_goalie, away_goalie

def convert_player_to_id(team_name: str, player_name: str):
    """
    converts a player name to id

    ...

    Parameters
    ----------
    team_name: str
        abbreviation for the players team
    player_name: str
        player name string. first and last name (ex. 'Olli Jokinen')

    Returns
    -------
    player_id: int
        player id
    """
    url = f'https://statsapi.web.nhl.com/api/v1/teams'
    resp = requests.get(url)
    json_data = json.loads(resp.text)

    for team in json_data['teams']:
        if team['abbreviation'] == team_name:
            team_id = team['id']
        else:
            continue
    # Use the team id to go to team page
    url = f'https://statsapi.web.nhl.com/api/v1/teams/{team_id}?expand=team.roster'
    resp = requests.get(url)
    json_data = json.loads(resp.text)

    team_roster = json_data['teams'][0]['roster']['roster']

    for p in team_roster:
        if p['person']['fullName'] == player_name:
            return p['person']['id']
        else:
            continue

if __name__ == '__main__':
    game_ids = get_game_ids(20192020)
    scrape_team_stats(game_ids[0])
    scrape_goalie_stats(game_ids[13])
    retrieve_team(game_ids[13],home=False)
    retrieve_date(game_ids[13])
    get_starting_goalies('FLA', 'CHI', '01-17-2021')
    convert_player_to_id('BUF', 'Taylor Hall')