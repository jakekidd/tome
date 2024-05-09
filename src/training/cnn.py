import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Conv2D, Flatten, Dropout, MaxPooling2D

class CNNModel:
    def __init__(self):
        # Initialize any required variables here
        self.model = None

    def preprocess_data(self, df):
        """
        Preprocess the order book data for CNN.

        Args:
            df (pandas.DataFrame): DataFrame containing the order book data.

        Returns:
            np.array: Processed data ready for training.
        """
        # Assuming the DataFrame 'df' has columns for different price levels and volumes
        # The structure is expected to be like df['bid_price_1'], df['ask_price_1'], df['bid_volume_1'], df['ask_volume_1'], etc.
        
        # Reshape data to fit a 2D grid format for CNN input
        # This is a simple example. Depending on your specific data, you might need to adjust the indices and range.
        
        # Number of price levels
        num_levels = 5  # Adjust this based on how many levels of the order book you have
        features_per_level = 4  # e.g., bid price, bid volume, ask price, ask volume

        # Create an empty array to hold the reshaped features
        cnn_input = np.zeros((len(df), num_levels, features_per_level))

        for i in range(num_levels):
            cnn_input[:, i, 0] = df[f'bid_price_{i+1}']
            cnn_input[:, i, 1] = df[f'bid_volume_{i+1}']
            cnn_input[:, i, 2] = df[f'ask_price_{i+1}']
            cnn_input[:, i, 3] = df[f'ask_volume_{i+1}']

        # Normalize the data if not already done - very important for CNN performance
        # Simple feature scaling to range [0, 1]
        cnn_input = cnn_input / cnn_input.max(axis=0)

        return cnn_input.reshape(len(df), num_levels, features_per_level, 1)  # Add channel dimension for CNN

    def build_model(self):
        """
        Build the CNN model architecture.
        """
        self.model = Sequential([
            Conv2D(32, kernel_size=(3, 3), activation='relu', input_shape=(5, 4, 1)),
            MaxPooling2D(pool_size=(2, 2)),
            Dropout(0.25),
            Conv2D(64, (3, 3), activation='relu'),
            MaxPooling2D(pool_size=(2, 2)),
            Dropout(0.25),
            Flatten(),
            Dense(128, activation='relu'),
            Dropout(0.5),
            Dense(1, activation='sigmoid')
        ])

        self.model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

# Example of using this class
if __name__ == "__main__":
    cnn = CNNModel()
    # Assume 'data_frame' is your DataFrame loaded with order book data
    processed_data = cnn.preprocess_data(data_frame)
    cnn.build_model()
    # You can now continue to train the model or further develop this script
