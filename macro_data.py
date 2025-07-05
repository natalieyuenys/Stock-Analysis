import requests
import pandas as pd
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv

class SharedMethods:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"

    def get_data(self, params):
        try:
            response = requests.get(self.base_url, params=params)
            data = response.json()
                                
            return data
        
        except requests.exceptions.RequestException as e:   
            print(f"Request failed: {e}")
            return None
            
class MacroDataExtractor(SharedMethods):
    def __init__(self, api_key=None):
        super().__init__(api_key)
        self.macro_functions = [
            'REAL_GDP', 'REAL_GDP_PER_CAPITA', 'FEDERAL_FUNDS_RATE',
            'UNEMPLOYMENT', 'INFLATION', 'INFLATION_EXPECTATION',
            'CPI', 'Treasury_Yield','RETAIL_SALES'
        ]
        self.treasury_maturities = [1, 2, 3, 5, 7, 10, 20, 30]

    def get_params(self, function, treasury_maturity):
        
        if function in ['REAL_GDP','REAL_GDP_PER_CAPITA'] :
            params = {
                'function': function,
                'interval': 'quarterly',
                'apikey': self.api_key,
            }
        if function in ['FEDERAL_FUNDS_RATE', 'CPI'] :
            params = {
                'function': function,
                'interval': 'monthly',
                'apikey': self.api_key,
            }
        elif function in ['UNEMPLOYMENT','INFLATION','INFLATION_EXPECTATION','RETAIL_SALES']:  
            params = {
                'function': function,
                'apikey': self.api_key,
            }
        elif function in ['Treasury_Yield']:
            params = {
                'function': function,
                'interval': 'daily',
                'maturity': "{}year".format(treasury_maturity),
                'apikey': self.api_key,
            }

        return params
    
    def extract_data(self,save_to_csv=True):
        results = pd.DataFrame()

        for function in self.macro_functions:
            print(f"Extracting data for {function}...")

            if function == 'Treasury_Yield':
                for maturity in self.treasury_maturities:
                    params = self.get_params(function, maturity)
                    data = self.get_data(params)
                    if data and 'data' in data:
                        df = pd.DataFrame(data['data'])
                        df['maturity'] = maturity
                        df['function'] = function
                        results = pd.concat([results, df], ignore_index=True)
            else:
                params = self.get_params(function, None)
                data = self.get_data(params)
                if data and 'data' in data:
                    df = pd.DataFrame(data['data'])
                    df['function'] = function
                    results = pd.concat([results, df], ignore_index=True)

        if save_to_csv and not results.empty:
            filename = f'data/macro_data.csv'
            results.to_csv(filename, index=False)
            print(f"\nData saved to: {filename}")

        return results        
    
if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not api_key: 
        print("Please set the ALPHA_VANTAGE_API_KEY environment variable.")

    macro_data_extractor = MacroDataExtractor(api_key)
    macro_data = macro_data_extractor.extract_data()