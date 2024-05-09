import sqlite3
import os

class DbManager:
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
                    timestamp1 TEXT NOT NULL,
                    timestamp2 TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    bid_price1 REAL,
                    bid_quantity1 REAL,
                    ask_price1 REAL,
                    ask_quantity1 REAL,
                    bid_price2 REAL,
                    bid_quantity2 REAL,
                    ask_price2 REAL,
                    ask_quantity2 REAL,
                    bid_price3 REAL,
                    bid_quantity3 REAL,
                    ask_price3 REAL,
                    ask_quantity3 REAL,
                    bid_price4 REAL,
                    bid_quantity4 REAL,
                    ask_price4 REAL,
                    ask_quantity4 REAL,
                    bid_price5 REAL,
                    bid_quantity5 REAL,
                    ask_price5 REAL,
                    ask_quantity5 REAL,
                    PRIMARY KEY (timestamp1, order_id, symbol)
                )
            ''')
            print("Table created successfully")
            conn.commit()

    def save(self, dataframe):
        """Saves fetched data to the database."""
        if 'exchange' in dataframe.columns:
            dataframe = dataframe.drop(columns=['exchange'])
        if 'symbol' in dataframe.columns:
            dataframe = dataframe.drop(columns=['symbol'])

        with sqlite3.connect(self.db_file) as conn:
            dataframe.to_sql('order_book', conn, if_exists='append', index=False)
            print("Data saved successfully to the database.")

# Example instantiation and use:
# if __name__ == "__main__":
#     db_manager = DbManager(Exchange.BINANCE, "ETH-USDT")
#     # Assume 'data' is a DataFrame containing data fetched by MarketDataFetcher
#     db_manager.save(data)
