import lightgbm as lgb
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.multioutput import MultiOutputRegressor
from optuna import create_study
from sklearn.decomposition import PCA

pd.reset_option('display.float_format', silent=True)
nft_trades = pd.read_csv('nft_traders_classified.csv')


y = nft_trades[['total_sol_amount_traded', 'avg_nb_hours_between_hold_trade']]
X = nft_trades.drop(['address', 'total_sol_amount_traded', 'avg_nb_hours_between_hold_trade'], axis=1)

X_train, X_test, y_train, y_test  = train_test_split(X, y, test_size=0.2, random_state=42)

def objective(trial):
    params = {
        "objective": "regression",
        "metric": "rmse",
        "n_estimators": 1000,
        "verbosity": -1,
        "bagging_freq": 1,
        "learning_rate": trial.suggest_float("learning_rate", 1e-3, 0.1, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 2, 2**10),
        "subsample": trial.suggest_float("subsample", 0.05, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.05, 1.0),
        "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 1, 100),
    }

    lgb_model = lgb.LGBMRegressor(**params)
    model = MultiOutputRegressor(lgb_model)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    rmse = mean_squared_error(y_test, predictions, squared=False)
    return rmse


# Optimize hyperparameters using Optuna
study = create_study(direction='minimize')
study.optimize(objective, n_trials=5)

# Train the model with the best hyperparameters
best_params = study.best_trial.params
best_model = lgb.LGBMRegressor(**best_params)
best_model = MultiOutputRegressor(best_model)
best_model.fit(X_train, y_train)

# Predict and evaluate
y_pred = best_model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
print(f"Optimized RMSE: {rmse}")


X_full = nft_trades.drop(['address', 'total_sol_amount_traded', 'avg_nb_hours_between_hold_trade'], axis=1)
y_full_pred = best_model.predict(X_full)
nft_trades['predicted_total_sol'] = y_full_pred[:, 0]
nft_trades['predicted_avg_hours'] = y_full_pred[:, 1]

nft_trades['predicted_total_sol'] = (nft_trades['predicted_total_sol'] - nft_trades['predicted_total_sol'].min()) / (nft_trades['predicted_total_sol'].max() - nft_trades['predicted_total_sol'].min())
nft_trades['predicted_avg_hours'] = (nft_trades['predicted_avg_hours'] - nft_trades['predicted_avg_hours'].min()) / (nft_trades['predicted_avg_hours'].max() - nft_trades['predicted_avg_hours'].min())

weight_total_sol = 0.5 
weight_avg_hours = 0.5 

# You might want to normalize these values before combining them, depending on their scale
nft_trades['combined_score'] = (nft_trades['predicted_total_sol'] * weight_total_sol) + \
                               (nft_trades['predicted_avg_hours'] * weight_avg_hours)
                               
nft_trades['rank'] = nft_trades['combined_score'].rank(ascending=False)

nft_trades.sort_values('rank').to_csv('nft_traders_ranked_gbm.csv',index=False)