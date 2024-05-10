from datetime import datetime, timedelta
from src.api.fetch import MarketDataFetcher, Exchange
from src.db.asset import AssetPairDbManager
import numpy as np
import pandas as pd

# Configuration constants
START_DATE = datetime(2020, 10, 10)
END_DATE = datetime(2023, 10, 10)
EXCHANGE = Exchange.BINANCE
ASSET_PAIR = "ETH-USDT"

def daily_data_collection():
    # Create instances of the fetcher and DB manager.
    fetcher = MarketDataFetcher(EXCHANGE, ASSET_PAIR)
    db = AssetPairDbManager(EXCHANGE, ASSET_PAIR)

    # Start collecting data from the start date until the end date.
    latest_date = db.get_latest_timestamp()
    current_date = latest_date + timedelta(days=1) if latest_date else START_DATE
    while current_date <= END_DATE:
        print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}...")
        
        # Define start and end datetime for each day to fetch.
        start_datetime = current_date # datetime(current_date.year, current_date.month, current_date.day)
        end_datetime = start_datetime + timedelta(days=1)

        # Fetch data using the MarketDataFetcher.
        try:
            fetcher.fetch(start_datetime, end_datetime)
        except Exception as e:
            print("Error fetching data for this round:", e)
            fetcher.data = fetcher.data.iloc[0:0] # Empty df.

        # Print column names
        # print("Column Names:", fetcher.data.columns.tolist())

        # Print the first row
        # print("First Row:", fetcher.data.iloc[0].to_dict())
        
        # Check if data was fetched and save it using DbManager.
        if not fetcher.data.empty:
            db.save(fetcher.data)
            print(f"Data for {current_date.strftime('%Y-%m-%d')} fetched and saved.")
        else:
            print(f"No data available for {current_date.strftime('%Y-%m-%d')}.")

        # Move to the next day.
        current_date += timedelta(days=1) 
        # current_date = db.get_latest_timestamp()

def interpolate(prior_data, following_data):
    """
    Leverages a gentle random walk and linear interpolation, which are conservative methods to estimate missing
    values. Here are some key aspects that help manage the variability and prevent dramatic price changes.
    """
    # Assuming 'prior_data' and 'following_data' are dataframes with your order book columns
    # and they are sorted by 'received_time'.
    
    interpolated_data = pd.DataFrame(columns=prior_data.columns)
    time_range = pd.date_range(start=prior_data['received_time'].iloc[-1],
                               end=following_data['received_time'].iloc[0],
                               freq='100L')  # 0.1 second frequency
    
    # Drop the exact endpoints since they are already in the data
    time_range = time_range[1:-1]  # Remove the first and last point
    
    for column in prior_data.columns:
        if 'price' in column or 'size' in column:
            start_value = prior_data[column].iloc[-1]
            end_value = following_data[column].iloc[0]
            if 'price' in column:
                # Create a random walk for prices
                steps = len(time_range)
                random_steps = np.random.normal(loc=0, scale=0.01, size=steps)  # Small price variation
                prices = np.linspace(start_value, end_value, steps + 2)[:-1] + np.cumsum(random_steps)
                interpolated_data[column] = prices[1:]  # avoid duplicating the last known data point
            else:
                # Linear interpolation for sizes
                sizes = np.linspace(start_value, end_value, len(time_range) + 2)[1:-1]
                interpolated_data[column] = sizes
    
    interpolated_data['received_time'] = time_range
    return interpolated_data

def imputate(start_date, end_date, db, fetcher):
    # Example: Simple forward fill or interpolation
    print(f"Imputating data from {start_date} to {end_date}")
    # Retrieve surrounding data for context
    prior_data = db.retrieve(start_date - timedelta(days=1), start_date)
    following_data = db.retrieve(end_date, end_date + timedelta(days=1))

    # Assuming prior_data and following_data are not empty and contain the needed fields
    if not prior_data.empty and not following_data.empty:
        # Calculate interpolated data here
        interpolated_data = interpolate(prior_data, following_data) #, start_date, end_date)
        db.save(interpolated_data)
    else:
        print("Not enough data to perform interpolation. Skipping.")

def fill_gaps():
    # Create instances of the fetcher and DB manager.
    fetcher = MarketDataFetcher(EXCHANGE, ASSET_PAIR)
    db = AssetPairDbManager(EXCHANGE, ASSET_PAIR)

    # Define the chunk size for checking gaps.
    chunk_size = timedelta(days=30)  # Check one month at a time.

    # Start and end date for the entire checking period.
    start_date = START_DATE
    end_date = END_DATE
    current_date = start_date

    while current_date < end_date:
        next_date = min(current_date + chunk_size, end_date)
        print(f"Checking data from {current_date} to {next_date}")

        timestamps = db.retrieve(current_date, next_date)

        if not timestamps:
            print(f"No data found from {current_date} to {next_date}. Filling the entire range.")
            try:
                fetcher.fetch(current_date, next_date)
                db.save(fetcher.data)
            except Exception as e:
                print(f"Error fetching data: {e}. Attempting to imputate missing data.")
                imputate(current_date, next_date, db, fetcher)
        else:
            # Identify and fetch data for gaps within the chunk
            for i in range(1, len(timestamps)):
                if (timestamps[i] - timestamps[i - 1]) > timedelta(minutes=1):
                    gap_start = timestamps[i - 1]
                    gap_end = timestamps[i]
                    print(f"Fetching missing data from {gap_start + timedelta(minutes=1)} to {gap_end}")
                    try:
                        fetcher.fetch(gap_start + timedelta(minutes=1), gap_end)
                        db.save(fetcher.data)
                    except Exception as e:
                        print(f"Error fetching data: {e}. Attempting to imputate missing data.")
                        imputate(gap_start + timedelta(minutes=1), gap_end, db, fetcher)

        # Move to the next chunk, ensuring no gap between chunks
        current_date = next_date

if __name__ == "__main__":
    daily_data_collection()
    # fill_gaps()
