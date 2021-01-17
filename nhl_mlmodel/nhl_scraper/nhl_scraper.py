# This module defines the functions to scrape the NHL.com API
from bs4 import BeautifulSoup
import datetime as dt
import json
import pickle
import requests
import sys
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

        """

    def __init__(self, date: dt.datetime, game_id: int, team: str, goals: int, pim: int, shots: int,
                 powerPlayPercentage: float, powerPlayGoals: int, powerPlayOpportunities: int,
                 faceOffWinPercentage: float, blocked: int, takeaways: int, giveaways: int, hits: int):
        self.date = date
        self.game_id = game_id
        self.team = team
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
    season_str = str(season)
    url = f"https://statsapi.web.nhl.com/api/v1/schedule?season={season_str}&gameType=R"
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

def retrieve_stats(game_id: int) -> List[NhlTeam]:
    """
        returns two entires in a List. The firs entry is for the home team and the second is the away team.
        Each entry represents 1 game played.

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

    url = f'https://statsapi.web.nhl.com/api/v1/game/{str(game_id)}/feed/live'
    resp = requests.get(url)
    json_data = json.loads(resp.text)

    # RETRIEVE STATS REQUIRED

    # Retrieve team names
    home_team = json_data["liveData"]['boxscore']['teams']['home']['team']['abbreviation']
    away_team = json_data["liveData"]['boxscore']['teams']['away']['team']['abbreviation']

    # Collect list of teamSkaterStats we want to retrieve from json data
    team_skater_stats_home = json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']
    team_skater_stats_away = json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']

    # Starting goalies
    # I spot checked a few APIs and it seems like the starting goalie will be listed last in the json
    # file if he was pulled. The goalie that finishes the game will be listed first (0).
    home_team_starting_goalie_id = json_data["liveData"]['boxscore']['teams']['home']['goalies'][-1]
    away_team_starting_goalie_id = json_data["liveData"]['boxscore']['teams']['away']['goalies'][-1]
    home_team_starting_goalie_name = json_data["liveData"]['boxscore']['teams']['home']['players']['ID'+str(home_team_starting_goalie_id)]['person']['fullName']
    away_team_starting_goalie_name = json_data["liveData"]['boxscore']['teams']['away']['players']['ID'+str(away_team_starting_goalie_id)]['person']['fullName']

    # Add Game ID, team names, starting goalies and date to home_skater_stats dictionary
    home_team_stats = NhlTeam(date=json_data['gameData']['datetime']['dateTime'], game_id=game_id,
                              team=home_team, goals=team_skater_stats_home['goals'],
                              pim=team_skater_stats_home['pim'], shots=team_skater_stats_home['shots'],
                              powerPlayPercentage=team_skater_stats_home['powerPlayPercentage'],
                              powerPlayGoals=team_skater_stats_home['powerPlayGoals'],
                              powerPlayOpportunities=team_skater_stats_home['powerPlayOpportunities'],
                              faceOffWinPercentage=team_skater_stats_home['faceOffWinPercentage'],
                              blocked=team_skater_stats_home['blocked'],
                              takeaways=team_skater_stats_home['takeaways'],
                              giveaways=team_skater_stats_home['giveaways'],
                              hits=team_skater_stats_home['hits'])
    team_skater_stats_home["Game ID"] = game_id
    team_skater_stats_home["Team"] = home_team
    team_skater_stats_home["Date"] = json_data['gameData']['datetime']['dateTime']
    team_skater_stats_home["Starting Goalie ID"] = home_team_starting_goalie_id
    team_skater_stats_home['Starting Goalie Name'] = home_team_starting_goalie_name
    team_skater_stats_home['Is Home Team'] = True

    # Retrieve outcome
    if json_data['liveData']['linescore']['hasShootout']==False:
        if json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']['goals'] > json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']['goals']:
            team_skater_stats_home['Home Team Win']=True
        if json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']['goals'] < json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']['goals']:
            team_skater_stats_home['Home Team Win']=False
    if json_data['liveData']['linescore']['hasShootout']==True:
        if json_data['liveData']['linescore']['shootoutInfo']['home']['scores'] > json_data['liveData']['linescore']['shootoutInfo']['away']['scores']:
            team_skater_stats_home['Home Team Win'] = True
        if json_data['liveData']['linescore']['shootoutInfo']['home']['scores'] < json_data['liveData']['linescore']['shootoutInfo']['away']['scores']:
            team_skater_stats_home['Home Team Win'] = False

    home_stats_dict = team_skater_stats_home

    # Add Game ID, team names, starting goalies and date to away_skater_stats dictionary
    team_skater_stats_away["Game ID"] = game_id
    team_skater_stats_away["Team"] = away_team
    team_skater_stats_away["Date"] = json_data['gameData']['datetime']['dateTime']
    team_skater_stats_away["Starting Goalie ID"] = away_team_starting_goalie_id
    team_skater_stats_away['Starting Goalie Name'] = away_team_starting_goalie_name
    team_skater_stats_away['Is Home Team'] = False
    # Retrieve outcome
    if json_data['liveData']['linescore']['hasShootout'] == False:
        if json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']['goals'] > \
                json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']['goals']:
            team_skater_stats_away['Home Team Win'] = True
        if json_data["liveData"]["boxscore"]['teams']['home']['teamStats']['teamSkaterStats']['goals'] < \
                json_data["liveData"]["boxscore"]['teams']['away']['teamStats']['teamSkaterStats']['goals']:
            team_skater_stats_away['Home Team Win'] = False
    if json_data['liveData']['linescore']['hasShootout'] == True:
        if json_data['liveData']['linescore']['shootoutInfo']['home']['scores'] > json_data['liveData']['linescore']['shootoutInfo']['away']['scores']:
            team_skater_stats_away['Home Team Win'] = True
        if json_data['liveData']['linescore']['shootoutInfo']['home']['scores'] < json_data['liveData']['linescore']['shootoutInfo']['away']['scores']:
            team_skater_stats_away['Home Team Win'] = False

    away_stats_dict = team_skater_stats_away

    # Rename dictionary keys with "Home" and "Away"
    #for i in team_skater_stats_home.keys():
        #team_skater_stats_home['Home '+str(i)] = team_skater_stats_home.pop(i)

    #for i in team_skater_stats_away.keys():
        #team_skater_stats_away['Away ' + str(i)] = team_skater_stats_away.pop(i)

    # Add dictionaries to list
    stats_dict = [home_stats_dict, away_stats_dict]

    return stats_dict


if __name__ == '__main__':
    game_ids = get_game_ids(20192020)
    retrieve_stats(game_ids[0])