import json
import pandas as pd
from datetime import datetime

def transform_to_ohlc(trades):
    df = pd.DataFrame(trades)
    df['price'] = df['price'].astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    ohlc = df.resample('1T', on='timestamp').agg({'price': ['first', 'max', 'min', 'last']})
    ohlc.columns = ['open', 'high', 'low', 'close']
    return ohlc

def read_trades():
    with open('data/trades.json', 'r') as f:
        trades = [json.loads(line) for line in f]    
    return trades

if __name__ == '__main__':
    trades = read_trades()
    ohlc_data = transform_to_ohlc(trades)
    ohlc_data.to_csv('data/ohlc.csv')