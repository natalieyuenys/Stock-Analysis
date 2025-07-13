import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
from functools import reduce

from sklearn.metrics import mean_squared_error
from sklearn.ensemble import  RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler

import warnings
warnings.filterwarnings("ignore")

def extract_data(symbol):

    global df_income, df_balance, df_cash_flow, df_earnings, df_overview, df_technical, df_price

    df_income = pd.read_csv('data/income_statement.csv')
    df_balance = pd.read_csv('data/balance_sheet.csv')
    df_cash_flow = pd.read_csv('data/cash_flow_statement.csv')   
    df_earnings = pd.read_csv('data/earnings.csv')   
    df_overview = pd.read_csv('data/company_overview.csv')
    df_technical = pd.read_csv('data/technical_data.csv')
    df_price = pd.read_csv('data/tech_stock_daily_price.csv')

    df_income['fiscalDateEnding'] = pd.to_datetime(df_income['fiscalDateEnding'])
    df_balance['fiscalDateEnding'] = pd.to_datetime(df_balance['fiscalDateEnding'])
    df_cash_flow['fiscalDateEnding'] = pd.to_datetime(df_cash_flow['fiscalDateEnding'])
    df_earnings['fiscalDateEnding'] = pd.to_datetime(df_earnings['fiscalDateEnding'])
    df_technical['date'] = pd.to_datetime(df_technical['date'])
    df_price['Date'] = pd.to_datetime(df_price['Date'])

    df_earnings = df_earnings[df_earnings['fiscalDateEnding'] >= '2005-01-01']
    df_technical = df_technical[df_technical['date']>='2005-01-01']
    df_price = df_price[df_price['Date'] >= '2005-01-01']

    df_income = df_income.sort_values(by=['symbol', 'fiscalDateEnding'])
    df_balance = df_balance.sort_values(by=['symbol', 'fiscalDateEnding'])
    df_cash_flow = df_cash_flow.sort_values(by=['symbol', 'fiscalDateEnding'])
    df_earnings = df_earnings.sort_values(by=['symbol', 'fiscalDateEnding'])
    df_technical = df_technical.sort_values(by=['ticker', 'date'])
    df_price = df_price.sort_values(by=['ticker', 'Date'])

    income_data = df_income[df_income['symbol'] == symbol]
    balance_data = df_balance[df_balance['symbol'] == symbol]
    overview_data = df_overview[df_overview['symbol'] == symbol]
    earnings_data = df_earnings[df_earnings['symbol'] == symbol]
    cash_flow_data = df_cash_flow[df_cash_flow['symbol'] == symbol]
    technical_data = df_technical[df_technical['ticker']==symbol]
    price_data = df_price[df_price['ticker'] == symbol]

    income_data = income_data.drop(columns=['symbol','reportedCurrency'])
    all_nan_columns = income_data.columns[income_data.isna().all()].tolist()
    income_data = income_data.drop(columns=all_nan_columns)

    balance_data = balance_data.drop(columns=['symbol','reportedCurrency'])
    all_nan_columns = balance_data.columns[balance_data.isna().all()].tolist()  
    balance_data = balance_data.drop(columns=all_nan_columns)

    cash_flow_data = cash_flow_data[['fiscalDateEnding','operatingCashflow']]
    earnings_data = earnings_data[['fiscalDateEnding','reportedEPS']]

    merged_df = reduce(lambda left, right: pd.merge(left, right, on='fiscalDateEnding'), [income_data, balance_data, cash_flow_data, earnings_data])

    merged_df = merged_df.sort_values(by='fiscalDateEnding')
    df = merged_df.merge(technical_data, left_on='fiscalDateEnding',right_on='date')
    df = price_data[['Date','Close']].merge(df, right_on='fiscalDateEnding', left_on='Date', how='left')
    
    df = df.fillna(method='ffill')
    df['Return'] = df['Close'].pct_change().fillna(0)
    lags = [1, 2, 3, 5, 10, 20, 30, 60, 90, 120, 180, 240]
    for lag in lags:
        df[f'return_lag{lag}'] = df['Return'].shift(lag)

    df = df[20:]  # Exclude the first 20 rows to avoid NaN values from lagging

    # Exclude Outliers
    df = df[(np.abs(df['Return'] - df['Return'].mean()) <= (3 * df['Return'].std()))]

    df['Direction'] = df['Return'].apply(lambda x: 1 if x > 0 else 0)
    df = df[df['Date']>='2005-04-01']

    df = df.drop(columns=['Date', 'date','fiscalDateEnding','ticker','company_name'])
    df = df.fillna(0)
    
    df_X = df.drop(columns=['Close','Return','Direction'])
    scaler = StandardScaler()
    df_X = scaler.fit_transform(df_X[df_X.columns])

    df_Y = df['Direction']

    X_train, X_test, y_train, y_test = train_test_split(
        df_X, df_Y, test_size=0.2, random_state=32
    )

    # 5. Initialize and fit the Random Forest model
    rf = RandomForestClassifier(
        n_estimators=100,  # Number of trees
        max_depth=None,    # Let trees expand until all leaves are pure
        random_state=42
    )
    rf.fit(X_train, y_train)

    y_pred = rf.predict(X_test)

    print("Accuracy:", accuracy_score(y_test, y_pred))
    print(classification_report(y_test, y_pred))
    print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))

if __name__ == "__main__":
    
    symbol = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'NFLX']
    for symbol in symbol:
        print(f"Processing data for {symbol}...")
        extract_data(symbol)
        print("=======================================================\n")