import requests
import pandas as pd
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from io import StringIO
from functools import reduce

class SharedMethods:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"

    def get_data(self, params, ticker):
        try:
            response = requests.get(self.base_url, params=params)
            data = response.json()
            
            if 'Error Message' in data:
                print(f"Error for {ticker}: {data['Error Message']}")
                return None
            elif 'Note' in data:
                print(f"Rate limit reached: {data['Note']}")
                return None
                
            return data
            
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return None
        


class TechnicalIndicatorExtractor(SharedMethods):

    def get_params(self, ticker, function, time_period):
        
        if function in ['EMA','WMA','RSI','SMA','BBANDS']:
            params = {
                'function': function,
                'symbol': ticker,
                'apikey': self.api_key,
                'interval': 'daily',
                'series_type': 'close',
                'time_period': time_period
                }
        
        elif function in ['VWAP','STOCH']:
            params = {
                'function': function,
                'symbol': ticker,
                'apikey': self.api_key,
                'interval': 'daily',
                }
        elif function == 'MACD':
            params = {
                'function': function,
                'symbol': ticker,
                'apikey': self.api_key,
                'interval': 'daily',
                'series_type': 'close',
                }
        elif function == 'WILLR':
            params = {
                'function': function,
                'symbol': ticker,
                'apikey': self.api_key,
                'interval': 'daily',
                'time_period': time_period
                }    
        
        return params
    
    def process_data(self, data, function, time_period):

        """Process the raw data into a DataFrame"""
        if 'Technical Analysis: {}'.format(function) not in data:
            print("No technical analysis data found.")
            return pd.DataFrame()

        column_name = 'Technical Analysis: {}'.format(function)
        df = pd.DataFrame.from_dict(data[column_name], orient='index')
        df = df.reset_index(drop=False)
        df = df.rename(columns={'index': 'date'})
        suffix_dict = {col: col + '_{}'.format(time_period) for col in df.columns if col != 'date'}
        df = df.rename(columns=suffix_dict)
        df['date'] = (pd.to_datetime(df['date'])).dt.strftime('%Y-%m-%d')
        df = df.sort_values(by='date')
        
        return df

    def extract_technical(self, df_ticker_list, save_to_csv=True):
        
        print("Starting extraction for companies...")
        
        
        results = pd.DataFrame()

        for i, (ticker, company_name) in enumerate(df_ticker_list.items(), 1):
            print(f"Processing {i}/{len(df_ticker_list)}: {ticker} ({company_name})")
            df_list = []
            for function in ['EMA','WMA','RSI','SMA','VWAP','MACD','STOCH','WILLR','BBANDS']:
                if function in ['EMA','WMA','RSI','SMA','BBANDS','WILLR']:
                    for time_period in [5, 10, 20, 30, 50, 100, 200]:
                        params = self.get_params(ticker, function, time_period)
                        data = self.get_data(params=params, ticker=ticker)
                        
                        if data:
                            df = self.process_data(data, function, time_period)
                            df_list.append(df)

                else:
                    params = self.get_params(ticker, function, None)
                    data = self.get_data(params=params, ticker=ticker)
                    
                    if data:
                        df = self.process_data(data, function, None)
                        df_list.append(df)
                            
            ticker_result = reduce(lambda left, right: pd.merge(left, right, on='date'), df_list)
            ticker_result['ticker'] = ticker
            ticker_result['company_name'] = company_name

            results = pd.concat([results, ticker_result],axis=0)

        if save_to_csv and not results.empty:
            filename = "data/technical_data_{}.csv".format(datetime.now().strftime('%Y%m%d_%H%M%S'))
            results.to_csv(filename, index=False)
            print(f"\nData saved to: {filename}")
        
        return results



if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not api_key: 
        print("Please set the ALPHA_VANTAGE_API_KEY environment variable.")

    # sector_list = SectorExtractor(api_key).extract_sector_data()
    df_ticker_list = pd.read_csv('data/sector_data.csv')
    df_ticker_list = df_ticker_list[df_ticker_list['Symbol'].isin(['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META', 'NVDA', 'TSLA', 'NFLX', 'INTC'])]
    df_ticker_list = df_ticker_list.set_index('Symbol')['Name'].to_dict()

    technical_df = TechnicalIndicatorExtractor(api_key).extract_technical(df_ticker_list, save_to_csv=True)

