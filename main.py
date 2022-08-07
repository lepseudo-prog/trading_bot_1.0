import api_keys
import time
from itertools import compress
from pybit import inverse_perpetual
import talib as ta
import pandas as pd
import numpy as np
#authentification with bybit API
session_auth = inverse_perpetual.HTTP(
    endpoint="https://api.bybit.com",
    api_key=api_keys.api_key,
    api_secret = api_keys.api_secret
)
#Set the timefrime to get candles
now = int(time.time())
min_40 = now - 2400
data = session_auth.query_kline(
    symbol="BTCUSD",
    interval="3",
    from_time=min_40
)
df = pd.DataFrame(data['result'])
#data fetched, need indicators now
df['rsi'] = ta.RSI(df['close'], timeperiod=9)
df['open_time'] = (pd.to_datetime(df['open_time'], unit='s'))
#pattern recognition
candle_names = ta.get_function_groups()['Pattern Recognition']
candle_rankings = {}
for i in range(len(candle_names)):
    candle_rankings[candle_names[i]+'_Bear'] = i + 1
    candle_rankings[candle_names[i]+'_Bull'] = i
for pattern in candle_names:
    df[pattern] = getattr(ta, pattern)(df['open'], df['high'], df['low'], df['close'])
df['candlestick_pattern'] = np.nan
df['candlestick_match_count'] = np.nan
for index, row in df.iterrows():
    # no pattern found
    if len(row[candle_names]) - sum(row[candle_names] == 0) == 0:
        df.loc[index,'candlestick_pattern'] = "NO_PATTERN"
        df.loc[index, 'candlestick_match_count'] = 0
    # single pattern found
    elif len(row[candle_names]) - sum(row[candle_names] == 0) == 1:
        # bull pattern 100 or 200
        if any(row[candle_names].values > 0):
            pattern = list(compress(row[candle_names].keys(), row[candle_names].values != 0))[0] + '_Bull'
            df.loc[index, 'candlestick_pattern'] = pattern
            df.loc[index, 'candlestick_match_count'] = 1
        # bear pattern -100 or -200
        else:
            pattern = list(compress(row[candle_names].keys(), row[candle_names].values != 0))[0] + '_Bear'
            df.loc[index, 'candlestick_pattern'] = pattern
            df.loc[index, 'candlestick_match_count'] = 1
        # multiple patterns matched -- select best performance
    else:
        # filter out pattern names from bool list of values
        patterns = list(compress(row[candle_names].keys(), row[candle_names].values != 0))
        container = []
        for pattern in patterns:
            if row[pattern] > 0:
                container.append(pattern + '_Bull')
            else:
                container.append(pattern + '_Bear')
        rank_list = [candle_rankings[p] for p in container]
        if len(rank_list) == len(container):
            rank_index_best = rank_list.index(min(rank_list))
            df.loc[index, 'candlestick_pattern'] = container[rank_index_best]
            df.loc[index, 'candlestick_match_count'] = len(container)
# clean up candle columns
df.drop(candle_names, axis = 1, inplace = True)
#end of pattern recognition


print(df)



