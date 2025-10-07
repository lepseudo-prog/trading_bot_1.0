import time
import pandas as pd
import requests
import json
import logging
from hyperliquid.info import Info
from hyperliquid.utils import constants

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_candles_sdk(coin="BTC", interval="1m", days_back=30):
    """
    Try fetching candles using hyperliquid-python-sdk.
    Returns DataFrame or None if fails.
    """
    try:
        info = Info(constants.MAINNET_API_URL, skip_ws=True)
        logger.info(f"Attempting SDK fetch for {interval} candles of {coin}...")

        end_time = int(time.time() * 1000)
        start_time = end_time - (days_back * 24 * 60 * 60 * 1000)
        all_candles = []
        current_start = start_time

        while current_start < end_time:
            # Try possible method names
            try:
                batch = info.candles_snapshot(coin, interval, current_start, end_time)
            except AttributeError:
                logger.warning("SDK method 'candles_snapshot' not found. Trying raw API.")
                return None
            if not batch:
                logger.warning("No more data in batch. Stopping.")
                break
            all_candles.extend(batch)
            current_start = batch[-1]['T'] + 1
            logger.info(f"Fetched {len(batch)} candles up to {pd.to_datetime(current_start, unit='ms')}")
            time.sleep(0.2)

        return all_candles

    except Exception as e:
        logger.error(f"SDK fetch failed: {str(e)}")
        return None

def fetch_candles_http(coin="BTC", interval="1m", days_back=30):
    """
    Fetch candles via raw HTTP POST to Hyperliquid mainnet.
    """
    try:
        url = "https://api.hyperliquid.xyz/info"
        headers = {"Content-Type": "application/json"}
        logger.info(f"Attempting HTTP fetch for {interval} candles of {coin}...")

        end_time = int(time.time() * 1000)
        start_time = end_time - (days_back * 24 * 60 * 60 * 1000)
        all_candles = []
        current_start = start_time

        while current_start < end_time:
            req_body = {
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": interval,
                    "startTime": current_start,
                    "endTime": end_time
                }
            }
            response = requests.post(url, headers=headers, data=json.dumps(req_body))
            if response.status_code != 200:
                logger.error(f"HTTP request failed: {response.status_code} - {response.text}")
                break
            batch = response.json()
            if not batch:
                logger.warning("No more data in batch. Stopping.")
                break
            all_candles.extend(batch)
            current_start = batch[-1]['T'] + 1
            logger.info(f"Fetched {len(batch)} candles up to {pd.to_datetime(current_start, unit='ms')}")
            time.sleep(0.2)

        return all_candles

    except Exception as e:
        logger.error(f"HTTP fetch failed: {str(e)}")
        return None

def process_candles(candles):
    """
    Convert candles to DataFrame and save to CSV.
    """
    if not candles:
        logger.error("No candles to process.")
        return pd.DataFrame()

    df = pd.DataFrame(candles)
    df['start_timestamp'] = pd.to_datetime(df['t'], unit='ms')
    df['end_timestamp'] = pd.to_datetime(df['T'], unit='ms')
    df = df[['start_timestamp', 'o', 'h', 'l', 'c', 'v', 'n', 'end_timestamp']].rename(columns={
        'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'n': 'trades_count'
    }).astype({
        'open': float, 'high': float, 'low': float, 'close': float, 'volume': float, 'trades_count': int
    })
    df.set_index('start_timestamp', inplace=True)
    df.sort_index(inplace=True)
    df = df.dropna()

    if df.empty:
        logger.error("DataFrame is empty after cleaning.")
        return df

    output_file = 'btc_1m_mainnet_data.csv'
    df.to_csv(output_file)
    logger.info(f"Saved {len(df)} candles to {output_file}")
    return df

def fetch_candles(coin="BTC", interval="1m", days_back=30):
    """
    Main function to fetch candles, trying SDK first then HTTP.
    """
    logger.info(f"Fetching {interval} candles for {coin} from {days_back} days ago...")
    candles = fetch_candles_sdk(coin, interval, days_back)
    if candles is None:
        logger.info("Falling back to HTTP method...")
        candles = fetch_candles_http(coin, interval, days_back)
    return process_candles(candles)

if __name__ == "__main__":
    df = fetch_candles(coin="BTC", interval="1m", days_back=30)
    if not df.empty:
        print(f"Preview of fetched data:\n{df.head()}")
        print(f"Data shape: {df.shape}")