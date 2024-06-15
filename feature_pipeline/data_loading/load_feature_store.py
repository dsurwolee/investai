import pandas as pd

def save_features():
    ohlc_data = pd.read_csv('data/ohlc.csv')
    ohlc_data.to_csv('data/ohlc_features.csv', index=False)

if __name__ == '__main__':
    save_features()

    
