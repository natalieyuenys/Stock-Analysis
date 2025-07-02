import requests
import pandas as pd
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv


class TechStockSentimentExtractor:
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
    
    def get_sentiment_data(self, ticker, limit=50, time_from=None, time_to=None):
        """Extract sentiment data using Alpha Vantage News Sentiment API"""
        
        params = {
            'function': 'NEWS_SENTIMENT',
            'tickers': ticker,
            'apikey': self.api_key,
            'limit': limit
        }
        
        if time_from:
            params['time_from'] = time_from
        if time_to:
            params['time_to'] = time_to
            
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

    def process_sentiment_score(self, news_data, ticker):
        """Calculate aggregate sentiment score from news data"""
        if not news_data or 'feed' not in news_data:
            return None
        
        records = []
        # Extract sentiment scores for the specific ticker
        for article in news_data.get('feed', []):
            # Find sentiment for the target company in ticker_sentiment
            ticker_sentiments = article.get('ticker_sentiment', [])
            for ts in ticker_sentiments:
                if ts.get('ticker') == ticker:
                    records.append({
                        'title': article.get('title'),
                        'url': article.get('url'),
                        'summary': article.get('summary'),
                        'source': article.get('source'),    
                        'published_date': article.get('time_published'),
                        'sentiment_score': ts.get('ticker_sentiment_score')
                    })

        df = pd.DataFrame(records)
        
        return df
    

    def extract_all_tech_sentiment(self, save_to_csv=True):
        """Extract sentiment data for all top tech companies"""
        results = pd.DataFrame()
        
        print("Starting sentiment extraction for top tech companies...")
        
        for i, (ticker, company_name) in enumerate(self.tech_tickers.items(), 1):
            print(f"Processing {i}/{len(self.tech_tickers)}: {ticker} ({company_name})")
            
            monthly_data = pd.DataFrame()
            for month in range(1, 7):
                datestart = '2025{:02d}01T0000'.format(month)
                dateend = '2025{:02d}01T0000'.format(month + 1) 
                sentiment_data = self.get_sentiment_data(ticker, limit=1000, time_from=datestart, time_to=dateend)
                
                if sentiment_data:
                    processed_data = self.process_sentiment_score(sentiment_data, ticker)
                    if not processed_data.empty:
                        processed_data['company_name'] = company_name
                        monthly_data = pd.concat([monthly_data, processed_data], axis=0)

            results = pd.concat([results,monthly_data], axis=0)
                

        if save_to_csv and not results.empty:
            filename = f'data/tech_stock_sentiment_by_article.csv'
            results.to_csv(filename, index=False)
            print(f"\nData saved to: {filename}")
        
        return results

if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not api_key: 
        print("Please set the ALPHA_VANTAGE_API_KEY environment variable.")

    extractor = TechStockSentimentExtractor(api_key)
    sentiment_df = extractor.extract_all_tech_sentiment(save_to_csv=True)