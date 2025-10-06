import time
import pandas as pd
from hyperliquid.info import Info
from hyperliquid.utils import constants

# Setup: Switch to mainnet for real data
info = Info(constants.MAINNET_API_URL, skip_ws=True) # skip_ws avoids websocket init