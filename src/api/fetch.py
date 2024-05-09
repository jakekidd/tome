import enum
from lakeapi import load_data
import pandas as pd
from tabulate import tabulate

class Exchange(enum.Enum):
    """Supported exchanges enumeration."""
    BINANCE = "BINANCE"
    BINANCE_FUTURES = "BINANCE_FUTURES"
    ASCENDEX = "ASCENDEX"
    KUCOIN = "KUCOIN"
    GATEIO = "GATEIO"
    SERUM = "SERUM"
    COINBASE = "COINBASE"
    HUOBI = "HUOBI"
    HUOBI_SWAP = "HUOBI_SWAP"
    OKX = "OKX"
    DYDX = "DYDX"
    BYBIT = "BYBIT"
    COINMATE = "COINMATE"

class MarketDataFetcher:
    def __init__(self, exchange, asset_pair):
        self.exchange = exchange
        self.asset_pair = asset_pair
        self.data = pd.DataFrame()  # Initialize an empty DataFrame

    def fetch(self, start, end):
        """
        Fetches data from the API and stores it in self.data.

        Args:
            start (datetime.datetime): The start datetime for the data.
            end (datetime.datetime): The end datetime for the data.
        """
        self.data = load_data(
            table="book",
            start=start,
            end=end,
            symbols=[self.asset_pair],
            exchanges=[self.exchange.value]
        )
        return self.data

    def print(self):
        """
        Prints the DataFrame stored in self.data in a tabulated format for better readability.
        """
        print(tabulate(self.data, headers='keys', tablefmt='psql', showindex="never"))
