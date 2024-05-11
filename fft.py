import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm

def load_data(db_path, start_date, end_date, columns):
    print("Connecting to database...")
    try:
        conn = sqlite3.connect(db_path)
        print("Database connection successful.")
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return None

    try:
        column_string = ', '.join(columns)
        query = f"""
        SELECT origin_time, {column_string}
        FROM order_book
        WHERE origin_time BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY origin_time;
        """
        print("Executing query...")
        cursor = conn.cursor()
        cursor.execute(query)
        print("1")
        col_names = [desc[0] for desc in cursor.description]  # Get column names from cursor
        data_frames = []
        while True:
            print("2")
            records = cursor.fetchmany(10000)
            if not records:
                break
            print("3")
            df = pd.DataFrame(records, columns=col_names)
            print("4")
            data_frames.append(df)
            print(f"Processed a chunk of size {len(records)}")
        data = pd.concat(data_frames)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Failed to execute query: {e}")
        return None
    finally:
        conn.close()
    return data

# Step 2: Prepare Data
def prepare_data(data):
    print("Preparing data...")
    data['origin_time'] = pd.to_datetime(data['origin_time'])
    data.set_index('origin_time', inplace=True)
    print("Data prepared.")
    return data

# Step 3: Compute FFT for all bid/ask prices and sizes
def compute_fft(data, columns):
    fft_results = {}
    print("Starting FFT computations...")
    for column in tqdm(columns, desc="Computing FFTs", unit="column"):
        fft_values = np.fft.fft(data[column].values)
        fft_freq = np.fft.fftfreq(len(fft_values), d=0.1)
        fft_results[column] = (fft_freq, fft_values)
    print("FFT computations completed.")
    return fft_results

# Step 4: Visualize Results
def plot_ffts(fft_results):
    n = len(fft_results)
    fig, axes = plt.subplots(nrows=n, ncols=1, figsize=(15, 2*n))
    for ax, (column, (fft_freq, fft_values)) in zip(axes, fft_results.items()):
        ax.stem(fft_freq, np.abs(fft_values), 'b', markerfmt=" ", basefmt="-b")
        ax.set_title(f'FFT of {column}')
        ax.set_xlim(0, 0.5)  # Limit to positive frequencies
        ax.grid()
    plt.tight_layout()
    plt.show()

# Main execution block
try:
    db_path = 'data/BINANCE_ETH_USDT.db'
    columns_needed = ['origin_time'] + [f'bid_{i}_price' for i in range(20)] + [f'ask_{i}_price' for i in range(20)]
    print("Script started.")
    data = load_data(db_path, '2022-01-01', '2022-01-02', columns_needed)
    if data is not None and not data.empty:
        prepared_data = prepare_data(data)
        fft_columns = [col for col in prepared_data.columns if 'bid_' in col or 'ask_' in col]
        fft_results = compute_fft(prepared_data, fft_columns)
        plot_ffts(fft_results)
    else:
        print("No data to process.")
except KeyboardInterrupt:
    print("Interrupted by user")
except Exception as e:
    print(f"An error occurred: {e}")

