import websocket
import json
import os

def on_message(ws, message):
    data = json.loads(message)
    if isinstance(data, list) and data[2] == "trade":        
        trade_data = {
            "price": data[1][0][0],
            "volume": data[1][0][1],
            "timestamp": data[1][0][2]
        }
        with open('data/trades.json', 'a') as f:
            f.write(json.dumps(trade_data) + '\n')

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send(json.dumps({
        'event': 'subscribe',
        'pair': ['XBT/USD'],
        'subscription': {'name': 'trade'}
    }))

if __name__ == '__main__':
    ws = websocket.WebSocketApp("wss://ws.kraken.com",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()