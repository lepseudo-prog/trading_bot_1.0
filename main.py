import api_keys
import bybit
import time
client = bybit.bybit(test=False, api_key=api_keys.api_key, api_secret=api_keys.api_secret)
print("Successful Login")

