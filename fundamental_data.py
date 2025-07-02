import requests
import pandas as pd
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from alpha_vantage.fundamentaldata import FundamentalData


class FundamentalDataExtractor:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"

        # Top tech company tickers for 2025
        self.tech_tickers = {
            'AAPL': 'Apple Inc.',
            'MSFT': 'Microsoft Corporation', 
            'GOOGL': 'Alphabet Inc.',
            'AMZN': 'Amazon.com Inc.',
            'META': 'Meta Platforms Inc.',
            'TSLA': 'Tesla Inc.',
            'NVDA': 'NVIDIA Corporation',
            'NFLX': 'Netflix Inc.',
            'CRM': 'Salesforce Inc.',
            'AMD': 'Advanced Micro Devices'
        }
    def get_fundamentals(self):
        fd = FundamentalData(key=self.api_key, output_format='pandas')
        income_dfs = []
        balance_dfs = []
        cashflow_dfs = []
        overview_dfs = []

        for i, (ticker, company_name) in enumerate(self.tech_tickers.items(), 1):
            print(f"Processing {i}/{len(Extractor.tech_tickers)}: {ticker} ({company_name})")

            try:
                # Income Statement
                income, _ = fd.get_income_statement_annual(symbol=ticker)
                income['symbol'] = ticker
                income_dfs.append(income)
                
                # Balance Sheet
                balance, _ = fd.get_balance_sheet_annual(symbol=ticker)
                balance['symbol'] = ticker
                balance_dfs.append(balance)
                
                # Cash Flow Statement
                cashflow, _ = fd.get_cash_flow_annual(symbol=ticker)
                cashflow['symbol'] = ticker
                cashflow_dfs.append(cashflow)
                
                # Company Overview
                overview, _ = fd.get_company_overview(symbol=ticker)
                overview['symbol'] = ticker
                overview_dfs.append(overview)
                
            except Exception as e:
                print(f"Error for {ticker}: {e}")

        # Concatenate each list of DataFrames into a single DataFrame
        df_income = pd.concat(income_dfs, ignore_index=True)
        df_balance = pd.concat(balance_dfs, ignore_index=True)
        df_cashflow = pd.concat(cashflow_dfs, ignore_index=True)
        df_overview = pd.concat(overview_dfs, ignore_index=True)

        return df_overview, df_income, df_balance, df_cashflow

if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("API key not found. Please set ALPHA_VANTAGE_API_KEY in your environment variables.")
    
    Extractor = FundamentalDataExtractor(api_key)
    
    overview, income, balance, cashflow = Extractor.get_fundamentals()

    overview.to_csv('data/company_overview.csv', index=False)
    income.to_csv('data/income_statement.csv', index=False)      
    balance.to_csv('data/balance_sheet.csv', index=False)
    cashflow.to_csv('data/cash_flow_statement.csv', index=False) 