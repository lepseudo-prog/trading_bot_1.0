import api_keys
import time
from pybit import inverse_perpetual
import talib
import pandas as pd
#authentification with bybit API
session_auth = inverse_perpetual.HTTP(
    endpoint="https://api.bybit.com",
    api_key=api_keys.api_key,
    api_secret = api_keys.api_secret
)
#Set the time to get only 30 last candles
now = int(time.time())
min_30 = now - 1800
data = session_auth.query_kline(
    symbol="BTCUSD",
    interval="1",
    from_time=min_30
)
df = pd.DataFrame(data['result'])
#data fetched, need indicators now

