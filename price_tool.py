import requests
import pandas as pd
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from io import StringIO

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
        
    def get_ticker_list(self):
        
        # params = {
        #     'function': 'LISTING_STATUS',
        #     'apikey': self.api_key
        # }

        # response = requests.get(self.base_url, params=params)
        # csv_data = response.text
        # df_active_ticker = pd.read_csv(StringIO(csv_data))
        # df_active_ticker = df_active_ticker[df_active_ticker['status'] == 'Active']
        # df_active_ticker = df_active_ticker[df_active_ticker['assetType'] == 'Stock']    

        # active_ticker_list = df_active_ticker['symbol'].tolist() 
        
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        active_ticker_list = sp500['Symbol'].tolist()
        return active_ticker_list

class SectorExtractor(SharedMethods):


    def get_sector_params(self, ticker):
      
        params = {
            'symbol': ticker,
            'function': 'OVERVIEW',
            'apikey': self.api_key
        }
        return params
    
    def extract_sector_data(self, sector=None, save_to_csv=True):

        ticker_list = self.get_ticker_list()
        print(f"Total tickers fetched: {len(ticker_list)}")
        
        result_df = pd.DataFrame()

        for ticker in ticker_list:
            params = self.get_sector_params(ticker)
            try:
                sector_data = self.get_data(params=params, ticker=ticker)
                df = pd.DataFrame([sector_data])[['Symbol','Name','Sector']]
            except:
                print(f"Error fetching data for {ticker}. Skipping...")
                continue

            result_df = pd.concat([result_df, df], ignore_index=True)

        if not sector==None:
            result_df = result_df[result_df['Sector'] == sector]

        if save_to_csv:
            filename = f'data/sector_data_{sector}.csv' if sector else 'data/sector_data.csv'
            result_df.to_csv(filename, index=False)
            print(f"\nData saved to: {filename}")
            
        return result_df        



class DailyAdjustedPriceExtractor(SharedMethods):

    def get_price_params(self, ticker, function='TIME_SERIES_DAILY_ADJUSTED', outputsize='full'):
        """Extract daily adjusted price data using Alpha Vantage API"""
        params = {
            'function': function,
            'symbol': ticker,
            'apikey': self.api_key,
            'outputsize': outputsize
        }
        return params
    
    def process_price_data(self, price_data):
        time_series = price_data['Time Series (Daily)']

        # Build a DataFrame of close prices
        df_close = pd.DataFrame(
            [(date, float(day_data['4. close'])) for date, day_data in time_series.items()],
            columns=['Date', 'Close']
        )
        df_close['Date'] = pd.to_datetime(df_close['Date'])

        return df_close

    def extract_price(self, df_ticker_list, save_to_csv=True):
        """Extract sentiment data for all top tech companies"""
        results = pd.DataFrame()
        
        print("Starting price extraction for top tech companies...")
        
        for i, (ticker, company_name) in enumerate(df_ticker_list.items(), 1):
            print(f"Processing {i}/{len(df_ticker_list)}: {ticker} ({company_name})")
            
            params = self.get_price_params(ticker)
            price_data = self.get_data(params=params, ticker=ticker)
            
            if price_data:
                df_price = self.process_price_data(price_data)
                if not df_price.empty:
                    df_price['ticker'] = ticker
                    df_price['company_name'] = company_name

            results = pd.concat([results,df_price], axis=0)
                

        if save_to_csv and not results.empty:
            filename = "data/SP500_daily_price_{}.csv".format(datetime.now().strftime('%Y%m%d_%H%M%S'))
            results.to_csv(filename, index=False)
            print(f"\nData saved to: {filename}")
        
        return results



if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not api_key: 
        print("Please set the ALPHA_VANTAGE_API_KEY environment variable.")

    # sector_list = SectorExtractor(api_key).extract_sector_data()
    # df_ticker_list = pd.read_csv('data/sector_data.csv')
    # df_ticker_list = df_ticker_list.set_index('Symbol')['Name'].to_dict()
    df_ticker_list = {'SPY':'SP500 ETF',}
    price_df = DailyAdjustedPriceExtractor(api_key).extract_price(df_ticker_list, save_to_csv=True)
