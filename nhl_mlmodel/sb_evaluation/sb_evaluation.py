# This module defines the functions for evaluating historical sportsbook results.

import pickle
import matplotlib.pyplot as plt
from nhl_mlmodel.covers_scraper import covers_scraper
import os
from sklearn.metrics import accuracy_score, brier_score_loss
from sklearn.calibration import calibration_curve
import sys
from typing import List, Tuple


def sportsbook_accuracy(game_data: List[covers_scraper.NhlGame]) -> float:
    """
    calculates the accuracy score of the sportsbooks for the provided games

    ...

    Parameters
    ----------
    game_data: List[NhlGame]
        list of NhlGame objects for which we want to calculate the accuracy score for

    Returns
    -------
    accuracy: float
        the accuracy of the sportsbooks predictions
    outcomes: List[Boolean]
        True if the home team wins
    predictions: List[Boolean]
        True if the sportsbook had the home team favoured
    probabilities: List[float]
        the implied probability of the home team winning based on the moneyline odds
    """
    outcomes = []  # The actual outcome of the game. True if the home team wins
    predictions = []  # The sportsbook's "prediction". True if the home team was favoured.
    probabilities = []  # The implied probabilities determined from the moneyline odds

    for d in game_data:
        moneyline = int(d.home_ml)
        home_score = int(d.home_score)
        away_score = int(d.away_score)

        if moneyline == 100:
            # We will exclude tossups for the calibration curve
            continue

        # Convert moneyline odds to their implied probabilities
        if moneyline < 0:
            probabilities.append(moneyline / (moneyline - 100))
        elif moneyline > 100:
            probabilities.append(100 / (moneyline + 100))

        outcomes.append(home_score > away_score)
        predictions.append(moneyline < 0)

    accuracy = 100 * accuracy_score(outcomes, predictions)

    return accuracy, outcomes, predictions, probabilities

def cal_curve(data, bins):
    """
    creates a calibration curve. X axis is the implied probability while the y axis is the percentage
    of time that prediction was correct

    ...

    Parameters
    ----------
    data: List[Tuple[List[Boolean], List[Boolean], List[Float], str]]
        the data that will be used to create the calibration curve. (Outcomes, Predictions, Probabilities, Name)

    Returns
    -------
    accuracy: float
        the accuracy of the sportsbooks predictions
    outcomes: List[Boolean]
        True if the home team wins
    predictions: List[Boolean]
        True if the sportsbook had the home team favoured
    probabilities: List[float]
        the implied probability of the home team winning based on the moneyline odds
    """

    fig = plt.figure(1, figsize=(12, 8))
    ax1 = plt.subplot2grid((3, 1), (0, 0), rowspan=2)
    ax2 = plt.subplot2grid((3, 1), (2, 0))

    ax1.plot([0, 1], [0, 1], "k:", label="Perfectly Calibrated")

    for y_test, y_pred, y_proba, name in data:
        brier = brier_score_loss(y_test, y_proba)
        print("{}\tAccuracy:{:.4f}\t Brier Loss: {:.4f}".format(
            name, accuracy_score(y_test, y_pred), brier))
        fraction_of_positives, mean_predicted_value = calibration_curve(y_test, y_proba, n_bins=bins)

        ax1.plot(mean_predicted_value, fraction_of_positives, label="%s (%1.4f)" % (name, brier))
        ax2.hist(y_proba, range=(0, 1), bins=bins, label=name, histtype="step", lw=2)

    ax1.set_ylabel("Fraction of positives")
    ax1.set_ylim([-0.05, 1.05])
    ax1.legend(loc="lower right")
    ax1.set_title('Calibration plots  (reliability curve)')

    ax2.set_xlabel("Mean predicted value")
    ax2.set_ylabel("Count")
    ax2.legend(loc="lower right")

    plt.tight_layout()

    fig.savefig('sb_evaluation.png')

if __name__ == '__main__':
    # get dates that nhl games were played on in the 2018/2019 season
    dates = covers_scraper.get_nhl_dates(20182019)

    games = [] # will hold NhlGame objects for all games played in the 2018/2019 season

    for d in dates:
        games += covers_scraper.nhl_games_date(d)

    accuracy, outcomes, predictions, probabilities = sportsbook_accuracy(games)
    data = [(outcomes, predictions, probabilities, 'Sportsbook')]

    # create calibration curve and save figure in data folder
    cal_curve(data,15)