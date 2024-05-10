import matplotlib.pyplot as plt
import matplotlib.animation as animation
from mplfinance.original_flavor import candlestick_ohlc
import datetime
import socket
import json

from common.sock import SockClient  # Ensure SockClient is implemented

class MarketUI:
    def __init__(self):
        self.client = SockClient()
        self.fig, self.ax = plt.subplots()
        
        # Setting up dark mode for matplotlib
        plt.style.use('dark_background')
        self.fig.patch.set_facecolor('#121212')
        self.ax.set_facecolor('#121212')
        self.ETH = 0
        self.USDT = 0

    def fetch_market_data(self):
        """ Fetch data from the socket. """
        data = self.client.receive_data()
        return json.loads(data)

    def update_plot(self, frame):
        """ Update plot with new data. """
        data = self.fetch_market_data()
        self.ETH = data['ETH']
        self.USDT = data['USDT']
        trades = data['trades']
        
        # Prepare data for candlestick chart
        ochl = [
            [datetime.datetime.strptime(trade['time'], '%Y-%m-%dT%H:%M:%S').timestamp(), trade['open'], trade['high'], trade['low'], trade['close']]
            for trade in trades
        ]
        
        # Clear the current axes
        self.ax.clear()
        candlestick_ohlc(self.ax, ochl, width=0.6, colorup='g', colordown='r')
        
        # Plot buy and sell points
        buys = [(trade['time'], trade['price']) for trade in trades if trade['type'] == 'buy']
        sells = [(trade['time'], trade['price']) for trade in trades if trade['type'] == 'sell']
        self.ax.plot([datetime.datetime.strptime(b[0], '%Y-%m-%dT%H:%M:%S').timestamp() for b in buys], [b[1] for b in buys], '^', markersize=10, color='g')
        self.ax.plot([datetime.datetime.strptime(s[0], '%Y-%m-%dT%H:%M:%S').timestamp() for s in sells], [s[1] for s in sells], 'v', markersize=10, color='r')

        # Set plot labels and title
        self.ax.set_xlabel('Time')
        self.ax.set_ylabel('Price')
        self.ax.set_title(f"Live Market Data for ETH-USDT: ETH={self.ETH}, USDT={self.USDT}")

        # Format timestamps for better readability on the x-axis
        self.ax.xaxis.set_major_formatter(plt.FuncFormatter(self.format_date))
        plt.xticks(rotation=45)
        plt.tight_layout()

    def format_date(self, x, pos=None):
        """ Format the date displayed on the x-axis. """
        return datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')

    def run(self):
        """ Run the UI application. """
        ani = animation.FuncAnimation(self.fig, self.update_plot, interval=1000)
        plt.show()

    def playback_history(self, speed=1.0):
        """ Simulate the playback of execution history at a given speed. """
        start_date = datetime.datetime.now() - datetime.timedelta(days=30)  # Arbitrary start date
        end_date = datetime.datetime.now()
        current_date = start_date

        while current_date <= end_date:
            trades = self.fetch_market_data_for_date(current_date)
            self.update_plot(trades)
            plt.pause(speed)  # Pause for 'speed' seconds
            current_date += datetime.timedelta(days=1)

if __name__ == '__main__':
    ui = MarketUI()
    ui.run()
