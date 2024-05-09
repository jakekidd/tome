import sqlite3
import os
import datetime
import pandas as pd

class DbManager:
    DESIRED_COLUMNS = [
        'received_time', 'sequence_number', 'origin_time',
        'bid_0_price', 'bid_0_size',
        'bid_1_price', 'bid_1_size',
        'bid_2_price', 'bid_2_size',
        'bid_3_price', 'bid_3_size',
        'bid_4_price', 'bid_4_size',
        'bid_5_price', 'bid_5_size',
        'bid_6_price', 'bid_6_size',
        'bid_7_price', 'bid_7_size',
        'bid_8_price', 'bid_8_size',
        'bid_9_price', 'bid_9_size',
        'bid_10_price', 'bid_10_size',
        'bid_11_price', 'bid_11_size',
        'bid_12_price', 'bid_12_size',
        'bid_13_price', 'bid_13_size',
        'bid_14_price', 'bid_14_size',
        'bid_15_price', 'bid_15_size',
        'bid_16_price', 'bid_16_size',
        'bid_17_price', 'bid_17_size',
        'bid_18_price', 'bid_18_size',
        'bid_19_price', 'bid_19_size',
        'ask_0_price', 'ask_0_size',
        'ask_1_price', 'ask_1_size',
        'ask_2_price', 'ask_2_size',
        'ask_3_price', 'ask_3_size',
        'ask_4_price', 'ask_4_size',
        'ask_5_price', 'ask_5_size',
        'ask_6_price', 'ask_6_size',
        'ask_7_price', 'ask_7_size',
        'ask_8_price', 'ask_8_size',
        'ask_9_price', 'ask_9_size',
        'ask_10_price', 'ask_10_size',
        'ask_11_price', 'ask_11_size',
        'ask_12_price', 'ask_12_size',
        'ask_13_price', 'ask_13_size',
        'ask_14_price', 'ask_14_size',
        'ask_15_price', 'ask_15_size',
        'ask_16_price', 'ask_16_size',
        'ask_17_price', 'ask_17_size',
        'ask_18_price', 'ask_18_size',
        'ask_19_price', 'ask_19_size'
    ]

    def __init__(self, exchange, asset_pair, db_path="data"):
        self.db_file = f"{db_path}/{exchange.value}_{asset_pair.replace('-', '_')}.db"
        self._ensure_db()

    def _ensure_db(self):
        """Ensures that the database file exists and sets up necessary tables."""
        os.makedirs(os.path.dirname(self.db_file), exist_ok=True)
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_book (
                    received_time TEXT NOT NULL,
                    sequence_number INTEGER,
                    origin_time TEXT NOT NULL,
                    bid_0_price REAL, bid_0_size REAL,
                    bid_1_price REAL, bid_1_size REAL,
                    bid_2_price REAL, bid_2_size REAL,
                    bid_3_price REAL, bid_3_size REAL,
                    bid_4_price REAL, bid_4_size REAL,
                    bid_5_price REAL, bid_5_size REAL,
                    bid_6_price REAL, bid_6_size REAL,
                    bid_7_price REAL, bid_7_size REAL,
                    bid_8_price REAL, bid_8_size REAL,
                    bid_9_price REAL, bid_9_size REAL,
                    bid_10_price REAL, bid_10_size REAL,
                    bid_11_price REAL, bid_11_size REAL,
                    bid_12_price REAL, bid_12_size REAL,
                    bid_13_price REAL, bid_13_size REAL,
                    bid_14_price REAL, bid_14_size REAL,
                    bid_15_price REAL, bid_15_size REAL,
                    bid_16_price REAL, bid_16_size REAL,
                    bid_17_price REAL, bid_17_size REAL,
                    bid_18_price REAL, bid_18_size REAL,
                    bid_19_price REAL, bid_19_size REAL,
                    ask_0_price REAL, ask_0_size REAL,
                    ask_1_price REAL, ask_1_size REAL,
                    ask_2_price REAL, ask_2_size REAL,
                    ask_3_price REAL, ask_3_size REAL,
                    ask_4_price REAL, ask_4_size REAL,
                    ask_5_price REAL, ask_5_size REAL,
                    ask_6_price REAL, ask_6_size REAL,
                    ask_7_price REAL, ask_7_size REAL,
                    ask_8_price REAL, ask_8_size REAL,
                    ask_9_price REAL, ask_9_size REAL,
                    ask_10_price REAL, ask_10_size REAL,
                    ask_11_price REAL, ask_11_size REAL,
                    ask_12_price REAL, ask_12_size REAL,
                    ask_13_price REAL, ask_13_size REAL,
                    ask_14_price REAL, ask_14_size REAL,
                    ask_15_price REAL, ask_15_size REAL,
                    ask_16_price REAL, ask_16_size REAL,
                    ask_17_price REAL, ask_17_size REAL,
                    ask_18_price REAL, ask_18_size REAL,
                    ask_19_price REAL, ask_19_size REAL,
                    PRIMARY KEY (received_time, sequence_number)
                )
            ''')
            print("Table created successfully")
            conn.commit()

    def save(self, dataframe):
        """Saves fetched data to the database."""

        print("TYPES", dataframe.dtypes)

        # Sanitize dataframe.
        dataframe = dataframe[DbManager.DESIRED_COLUMNS]

        # Convert pandas Timestamp to string or another compatible format.
        for col in dataframe.select_dtypes(include=[pd.Timestamp]).columns:
            dataframe[col] = dataframe[col].astype(str)  # Convert Timestamp to string.

        # Ensure all data is in a format SQLite can handle.
        dataframe = dataframe.where(pd.notnull(dataframe), None)  # Replace NaN with None.
        for column in dataframe.columns:
            if isinstance(dataframe[column].dtype, pd.Timestamp):
                dataframe[column] = dataframe[column].astype(str)
            elif dataframe[column].dtype == 'object':
                dataframe[column] = dataframe[column].astype(str) # Ensure all objects are strings.

        # with sqlite3.connect(self.db_file) as conn:
        #     dataframe.to_sql('order_book', conn, if_exists='append', index=False)
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            # Prepare an SQL statement for insertion.
            columns = ', '.join(dataframe.columns)
            placeholders = ', '.join(['?'] * len(dataframe.columns))
            sql = f"INSERT OR IGNORE INTO order_book ({columns}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples.
            data_tuples = [tuple(x) for x in dataframe.to_numpy()]
            
            # Execute SQL for each row.
            cursor.executemany(sql, data_tuples)
            conn.commit()
            print("Data saved successfully to the database.")

    def get_latest_timestamp(self):
        """
        Retrieves the latest timestamp from the order_book table.
        Returns None if no data has been gathered yet.
        """
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(received_time) FROM order_book")
            latest_timestamp = cursor.fetchone()[0]  # This will be None if no rows are returned
            if latest_timestamp is not None:
                # Assuming the timestamp is stored in ISO format (e.g., '2020-10-10T14:00:00')
                return datetime.datetime.fromisoformat(latest_timestamp)
            return None

# Example instantiation and use:
# if __name__ == "__main__":
#     db_manager = DbManager(Exchange.BINANCE, "ETH-USDT")
#     # Assume 'data' is a DataFrame containing data fetched by MarketDataFetcher
#     db_manager.save(data)
