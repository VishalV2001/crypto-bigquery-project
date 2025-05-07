import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import random
from dotenv import load_dotenv

# Load .env
load_dotenv()

output_dir = "all_files"
os.makedirs(output_dir, exist_ok=True)

### ------------------ CRYPTOCOMPARE SECTION ------------------ ###
cryptocompare_tokens = ["MAX"]

def fetch_marketcap_price(token):
    api_key = os.getenv("CRYPTOCOMPARE_API_KEY")
    url = "https://min-api.cryptocompare.com/data/pricemultifull"
    headers = {'authorization': f'Apikey {api_key}'}
    params = {'fsyms': token, 'tsyms': 'USD'}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        try:
            data = response.json()['RAW'][token]['USD']
            return data.get('MKTCAP', None), data.get('PRICE', None)
        except KeyError:
            return None, None
    else:
        return None, None

def fetch_from_cryptocompare(token, circulating_supply):
    api_key = os.getenv("CRYPTOCOMPARE_API_KEY")
    url = "https://min-api.cryptocompare.com/data/v2/histominute"
    headers = {'authorization': f'Apikey {api_key}'}
    all_data = []
    limit = 2000
    aggregate = 5
    now = int(time.time())
    data_points_needed = 8640  # 30 days of 5-min candles

    while data_points_needed > 0:
        params = {
            'fsym': token, 'tsym': 'USD', 'limit': min(limit, data_points_needed) - 1,
            'aggregate': aggregate, 'toTs': now
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                batch_data = response.json()['Data']['Data']
                if not batch_data:
                    break
                all_data.extend(batch_data)
                now = batch_data[0]['time'] - 1
                data_points_needed -= len(batch_data)
            except KeyError:
                break
        else:
            break
        time.sleep(1)

    df = pd.DataFrame(all_data)
    if df.empty:
        return None

    df['timestamp'] = pd.to_datetime(df['time'], unit='s')
    df = df.sort_values(by="timestamp", ascending=False)
    df['token'] = token
    df['marketcap'] = df['close'] * circulating_supply
    df.rename(columns={'volumefrom': 'volume'}, inplace=True)

    return df[['token', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'marketcap']]

def fetch_all_cryptocompare_data():
    for token in cryptocompare_tokens:
        print(f"[CryptoCompare] Fetching marketcap and price for {token}")
        marketcap, price = fetch_marketcap_price(token)
        if marketcap is None or price in [None, 0]:
            print(f"Skipping {token} (missing marketcap or price)")
            continue
        supply = marketcap / price
        df = fetch_from_cryptocompare(token, supply)
        if df is not None:
            df = df.sort_values(by="timestamp", ascending=False)
            df.to_csv(os.path.join(output_dir, f"{token}.csv"), index=False)
            print(f"[CryptoCompare] Saved: {token}.csv")
        else:
            print(f"[CryptoCompare] Failed to fetch {token}")

### ------------------ BINANCE SECTION ------------------ ###
binance_token_symbols = {
    "BANANAS31": "BANANAS31USDT", "ETHDYDX": "DYDXUSDT", "FORM": "FORMUSDT",
    "GHST": "GHSTUSDT", "MLN": "MLNUSDT", "RED": "REDUSDT", "WAXP": "WAXPUSDT"
}

def fetch_binance_data():
    now = datetime.utcnow()
    thirty_days_ago = now - timedelta(days=30)
    start_ts = int(thirty_days_ago.timestamp() * 1000)
    end_ts = int(now.timestamp() * 1000)

    for token_name, symbol in binance_token_symbols.items():
        print(f"[Binance] Fetching 5-minute OHLCV for {token_name}")
        start = start_ts
        output = []
        try:
            while start < end_ts:
                res = requests.get(
                    "https://api.binance.com/api/v3/klines",
                    params={"symbol": symbol, "interval": "5m", "limit": 1000, "startTime": start}
                )
                res.raise_for_status()
                klines = res.json()
                if not klines:
                    break
                for k in klines:
                    timestamp = datetime.utcfromtimestamp(k[0] / 1000)
                    if timestamp < thirty_days_ago:
                        continue
                    open_, high, low, close, volume = map(float, k[1:6])
                    avg_price = (high + low) / 2
                    marketcap = avg_price * volume
                    output.append({
                        "token": token_name,
                        "timestamp": timestamp,
                        "open": open_,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": volume,
                        "marketcap": marketcap
                    })
                start = klines[-1][0] + 5 * 60 * 1000
                time.sleep(random.uniform(0.6, 1.2))
            if output:
                output_df = pd.DataFrame(output).sort_values(by="timestamp", ascending=False)
                output_df.to_csv(os.path.join(output_dir, f"{token_name}.csv"), index=False)
                print(f"[Binance] Saved: {token_name}.csv")
            else:
                print(f"[Binance] No data for {token_name}")
        except Exception as e:
            print(f"[Binance] Failed {token_name}: {e}")
        time.sleep(random.uniform(1.2, 2.5))

### ------------------ KUCOIN SECTION ------------------ ###
kucoin_tokens = ["MEMEFI", "AERGO", "JELLYJELLY", "ZEREBRO", "BABY", "BIGTIME", "BMT", "BNB", "BTC", "DGB", "DYDX", "FORTH", "GMT",
                 "GUN", "HIGH", "KDA", "KERNEL", "MAX", "MEME", "MUBARAK", "NEIRO", "NIL", "PAXG", "PARTI", "PROM", "PROMPT", "QKC", 
                 "STRAX", "TUT", "WCT"]

def calculate_marketcap(high, low, volume):
    return ((float(high) + float(low)) / 2) * float(volume)

def fetch_ohlcv(symbol, interval='5min', start_time=None, end_time=None):
    url = "https://api.kucoin.com/api/v1/market/candles"
    params = {"symbol": symbol, "type": interval, "startAt": start_time, "endAt": end_time}
    response = requests.get(url, params=params)
    data = response.json()

    if data['code'] != '200000':
        raise Exception("API error: " + str(data))
    candles = data['data']
    return sorted(candles, key=lambda x: int(x[0]), reverse=True)

def fetch_kucoin_data():
    for token in kucoin_tokens:
        print(f"[KuCoin] Fetching OHLCV for {token}")
        output = []
        try:
            symbol = f"{token}-USDT"
            
            # Defined 4 requests for 30 days of data (7 days per request)
            end_time = int(time.time())
            start_time = end_time - (7 * 24 * 60 * 60)  # 7 days ago

            for i in range(4):  # 4 requests for 4 weeks (30 days)
                candles = fetch_ohlcv(symbol, start_time=start_time, end_time=end_time)
                for entry in candles:
                    timestamp = datetime.fromtimestamp(int(entry[0]))
                    open_, close, high, low, volume, _ = entry[1:7]
                    marketcap = calculate_marketcap(high, low, volume)
                    output.append({
                        "token": token,
                        "timestamp": timestamp,
                        "open": float(open_),
                        "high": float(high),
                        "low": float(low),
                        "close": float(close),
                        "volume": float(volume),
                        "marketcap": marketcap
                    })
                
                # Move the time range for the next request
                start_time -= (7 * 24 * 60 * 60)  # Subtract another week
                end_time -= (7 * 24 * 60 * 60)    # Subtract another week

            if output:
                output_df = pd.DataFrame(output).sort_values(by="timestamp", ascending=False)
                output_df.to_csv(os.path.join(output_dir, f"{token}.csv"), index=False)
                print(f"[KuCoin] Saved: {token}.csv")
            else:
                print(f"[KuCoin] No data for {token}")
        except Exception as e:
            print(f"[KuCoin] Failed {token}: {e}")
        time.sleep(random.uniform(0.5, 1.5))


### ------------------ MAIN EXECUTION ------------------ ###
if __name__ == '__main__':
    print("Fetching data from CryptoCompare...")
    fetch_all_cryptocompare_data()

    print("Fetching data from Binance...")
    fetch_binance_data()

    print("Fetching data from KuCoin...")
    fetch_kucoin_data()

    print(f"All individual token files saved in the '{output_dir}' folder.")
