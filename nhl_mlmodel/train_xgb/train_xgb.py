from hyperopt import fmin, tpe, hp, Trials
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pickle
from sklearn.metrics import accuracy_score, brier_score_loss
from sklearn.calibration import calibration_curve
import xgboost as xgb
import sys

# show full columns on dfs
pd.set_option('display.expand_frame_repr', False)
pd.set_option("display.max_rows", 25)

def cal_curve(data, bins, name):
    # obtained from:
    #https://scikit-learn.org/stable/auto_examples/calibration/plot_calibration_curve.html
    fig = plt.figure(1, figsize=(12, 8))
    ax1 = plt.subplot2grid((3, 1), (0, 0), rowspan=2)
    ax2 = plt.subplot2grid((3, 1), (2, 0))

    ax1.plot([0, 1], [0, 1], "k:", label="Perfectly calibrated")

    for y_test, y_pred, y_proba, name in data:
        brier = brier_score_loss(y_test, y_proba)
        print("{}\t\tAccuracy:{:.4f}\t Brier Loss: {:.4f}".format(
            name, accuracy_score(y_test, y_pred), brier))
        fraction_of_positives, mean_predicted_value = \
            calibration_curve(y_test, y_proba, n_bins=bins)
        ax1.plot(mean_predicted_value, fraction_of_positives,
                 label="%s (%1.4f)" % (name, brier))
        ax2.hist(y_proba, range=(0, 1), bins=bins, label=name,
                 histtype="step", lw=2)

    ax1.set_ylabel("Fraction of positives")
    ax1.set_ylim([-0.05, 1.05])
    ax1.legend(loc="lower right")
    ax1.set_title('Calibration plots  (reliability curve)')

    ax2.set_xlabel("Mean predicted value")
    ax2.set_ylabel("Count")
    ax2.legend(loc="lower right")

    plt.tight_layout()
    fig.savefig(name)

if __name__ == '__main__':
    '''This module will train and evaluate an xgboost model along with hyper parameter optimization'''
    pd.set_option('display.max_columns', None)
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/games_df.pkl', 'rb') as f:
        df = pickle.load(f)

    # Shuffle data
    np.random.seed(42)
    df = df.reindex(np.random.permutation(df.index))
    df.reset_index(inplace=True, drop=True)  # Reset index

    # create test, train splits
    X = df.drop(columns=['game_id', 'home_team', 'away_team', 'date', 'home_goalie_id', 'away_goalie_id',
                         'home_team_win', 'home_goalie_name', 'away_goalie_name'])
    y = df.home_team_win

    X_train = X[:-2500]
    y_train = y[:-2500]
    X_valid = X[-2500:-500]
    y_valid = y[-2500:-500]
    X_test = X[-500:]
    y_test = y[-500:]

    # Train Model without optimizing hyperparameters
    params = {'learning_rate': 0.05, 'max_depth': 5}
    gbm = xgb.XGBClassifier(**params)
    model = gbm.fit(X_train, y_train,
                    eval_set=[[X_train, y_train],
                              [X_valid, y_valid]],
                    eval_metric='logloss',
                    early_stopping_rounds=10)
    xgb_test_preds = model.predict(X_test)
    xgb_test_proba = model.predict_proba(X_test)[:, 1]

    # Evaluate model
    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/baseline.pkl', 'rb') as f:
        outcomes, predictions, probabilities = pickle.load(f)

    data = [
        (outcomes, predictions, probabilities, 'Sportsbook'),
        (y_test, xgb_test_preds, xgb_test_proba, 'XGBoostStd')
    ]
    cal_curve(data, 15, 'unoptimized.png')

    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/xgb_model_unopt.pkl', 'wb') as f:
        pickle.dump(model, f)


    ######### Hyper Parameter Optimization Functions #########

    def get_xgb_model(params):
        '''This function will train the xgboost model'''
        # comment the next 2 lines out to disable gpu
        # params['gpu_id'] = 0
        # params['tree_method'] = 'gpu_hist'
        params['seed'] = 13

        gbm = xgb.XGBClassifier(**params, n_estimators=999)
        model = gbm.fit(X_train, y_train,
                        verbose=False,
                        eval_set=[[X_train, y_train],
                                  [X_valid, y_valid]],
                        eval_metric='logloss',
                        early_stopping_rounds=15)
        return model


    def xgb_objective(params):
        '''This function will evaluate the trained xgboost model based on the brier loss score'''
        params['max_depth'] = int(params['max_depth'])
        model = get_xgb_model(params)
        xgb_test_proba = model.predict_proba(X_valid)[:, 1]
        score = brier_score_loss(y_valid, xgb_test_proba)
        return (score)


    trials = Trials()


    def get_xgbparams(space, evals=15):
        '''This function will tune hyperparameters'''
        params = fmin(xgb_objective,
                      space=space,
                      algo=tpe.suggest,
                      max_evals=evals,
                      trials=trials)
        params['max_depth'] = int(params['max_depth'])
        return params


    ######################################################

    # Train Model and optimize hyper parameters
    hyperopt_runs = 100

    space = {
        'max_depth': hp.quniform('max_depth', 1, 8, 1),
        'min_child_weight': hp.quniform('min_child_weight', 3, 15, 1),
        'learning_rate': hp.qloguniform('learning_rate', np.log(.01), np.log(.1), .01),
        'subsample': hp.quniform('subsample', 0.5, 1.0, .1),
        'colsample_bytree': hp.quniform('colsample_bytree', 0.5, 1.0, .1),
        'reg_alpha': hp.qloguniform('reg_alpha', np.log(1e-2), np.log(1e2), 1e-2)
    }
    xgb_params = get_xgbparams(space, hyperopt_runs)
    xgb_params
    pickle.dump(trials, open('xgb_params.pkl', 'wb'))

    # Evaluate model
    model = get_xgb_model(xgb_params)
    xgb_test_preds = model.predict(X_test)
    xgb_test_proba = model.predict_proba(X_test)[:, 1]

    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/baseline.pkl', 'rb') as f:
        outcomes, predictions, probabilities = pickle.load(f)

    data = [
        (outcomes, predictions, probabilities, 'Sportsbook'),
        (y_test, xgb_test_preds, xgb_test_proba, 'XGBoostOpt')
    ]
    cal_curve(data, 15, 'optimized.png')

    with open('/Users/patrickpetanca/PycharmProjects/nhl_mlmodel/data/xgb_model_opt.pkl', 'wb') as f:
        pickle.dump(model, f)