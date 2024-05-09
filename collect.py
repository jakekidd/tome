from datetime import datetime, timedelta
from src.api.fetch import MarketDataFetcher, Exchange
from src.db.manager import DbManager

# Configuration constants
START_DATE = datetime(2020, 10, 10)
END_DATE = datetime(2023, 10, 10)
EXCHANGE = Exchange.BINANCE
ASSET_PAIR = "ETH-USDT"

def daily_data_collection():
    # Create instances of the fetcher and DB manager
    fetcher = MarketDataFetcher(EXCHANGE, ASSET_PAIR)
    db = DbManager(EXCHANGE, ASSET_PAIR)

    # Start collecting data from the start date until the end date
    current_date = START_DATE
    while current_date <= END_DATE:
        print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}...")
        
        # Define start and end datetime for each day to fetch
        start_datetime = datetime(current_date.year, current_date.month, current_date.day)
        end_datetime = start_datetime + timedelta(days=1)
        
        # Fetch data using the MarketDataFetcher
        fetcher.fetch("order_book", start_datetime, end_datetime)
        
        # Check if data was fetched and save it using DbManager
        if not fetcher.data.empty:
            db.save(fetcher.data)
            print(f"Data for {current_date.strftime('%Y-%m-%d')} fetched and saved.")
        else:
            print(f"No data available for {current_date.strftime('%Y-%m-%d')}.")

        # Move to the next day
        current_date += timedelta(days=1)

if __name__ == "__main__":
    daily_data_collection()
