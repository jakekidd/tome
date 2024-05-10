import sqlite3
import os
import pandas as pd

class DbManager:
    def __init__(self, db_filename, db_path="data"):
        self.db_file = f"{db_path}/{db_filename}"
        self._ensure_db()

    @property
    def create_query(self):
        """ Property to get the SQL query for creating tables. Should be overridden by subclasses. """
        pass

    @property
    def retrieve_query(self):
        """ Property to get the SQL query for retrieving data. Should be overridden by subclasses. """
        pass

    @property
    def desired_columns(self):
        """ Property to get the list of desired columns for the database. Should be overridden by subclasses. """
        pass

    def _ensure_db(self):
        """Ensures that the database file exists and sets up necessary tables."""
        os.makedirs(os.path.dirname(self.db_file), exist_ok=True)
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(self.create_query)
            print("Table created successfully")
            conn.commit()

    def save(self, dataframe: pd.DataFrame):
        """Saves fetched data to the database."""

        # Ensure all datetime columns are converted to ISO format strings
        datetime_cols = dataframe.select_dtypes(include=['datetime64[ns]']).columns
        for col in datetime_cols:
            dataframe[col] = dataframe[col].dt.to_pydatetime()  # Convert to Python datetime objects
            dataframe[col] = dataframe[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        # Replace NaN with None for proper null handling in SQL
        dataframe = dataframe.where(pd.notnull(dataframe), None)

        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            # Prepare an SQL statement for insertion
            columns = ', '.join(dataframe.columns)
            placeholders = ', '.join(['?'] * len(dataframe.columns))
            sql = f"INSERT OR IGNORE INTO order_book ({columns}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples
            data_tuples = [tuple(x) for x in dataframe.to_numpy()]

            # Execute SQL for each row
            try:
                cursor.executemany(sql, data_tuples)
            except sqlite3.InterfaceError as e:
                # Debugging output to identify the problematic data
                for dt in data_tuples:
                    try:
                        cursor.execute(sql, dt)
                    except sqlite3.InterfaceError:
                        print("Failed data tuple:", dt)  # Print problematic tuple
                        raise
            conn.commit()
            print("Data saved successfully to the database.")

    def retrieve(self, start, end):
        """ Retrieves timestamps within a specific range to check for data gaps. """
        with sqlite3.connect(self.db_file) as conn:
            query = self.retrieve_query
            df = pd.read_sql(query, conn, params=(start.isoformat(), end.isoformat()), parse_dates=['received_time'])
        return df['received_time'].tolist()

# Example instantiation and use:
# if __name__ == "__main__":
#     db_manager = DbManager(Exchange.BINANCE, "ETH-USDT")
#     # Assume 'data' is a DataFrame containing data fetched by MarketDataFetcher
#     db_manager.save(data)
