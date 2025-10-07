import pandas as pd
import numpy as np
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def detect_order_blocks(df, lookback=20, volume_threshold=1.5):
    """
    Detect Order Blocks (OB): High-volume reversal zones.
    - Pivot highs/lows with volume above threshold.
    Returns df with 'ob_high' and 'ob_low' binary flags.
    """
    # Calculate pivots (centered rolling for accuracy)
    df['pivot_high'] = df['high'].rolling(window=lookback, center=True).max() == df['high']
    df['pivot_low'] = df['low'].rolling(window=lookback, center=True).min() == df['low']
    
    # Average volume for threshold
    avg_vol = df['volume'].rolling(window=lookback).mean()
    
    # Flag OBs: Pivot + high volume
    df['ob_high'] = df['pivot_high'] & (df['volume'] > avg_vol * volume_threshold)
    df['ob_low'] = df['pivot_low'] & (df['volume'] > avg_vol * volume_threshold)
    
    # Drop temp columns
    df.drop(['pivot_high', 'pivot_low'], axis=1, inplace=True)
    
    return df

def detect_fair_value_gaps(df, gap_threshold=0.001):
    """
    Detect Fair Value Gaps (FVG): Imbalances between candles.
    - Bullish FVG: low_t > high_t-1
    - Bearish FVG: high_t < low_t-1
    Returns df with 'fvg_bull' and 'fvg_bear' (gap size if exists, else 0).
    """
    # Shifted previous highs/lows
    prev_high = df['high'].shift(1)
    prev_low = df['low'].shift(1)
    
    # Bullish FVG: Current low > prev high (gap up)
    df['fvg_bull'] = np.where(df['low'] > prev_high, (df['low'] - prev_high) / prev_high, 0)
    
    # Bearish FVG: Current high < prev low (gap down)
    df['fvg_bear'] = np.where(df['high'] < prev_low, (prev_low - df['high']) / prev_low, 0)
    
    # Filter small gaps
    df['fvg_bull'] = np.where(df['fvg_bull'] > gap_threshold, df['fvg_bull'], 0)
    df['fvg_bear'] = np.where(df['fvg_bear'] > gap_threshold, df['fvg_bear'], 0)
    
    return df

def detect_liquidity_sweeps(df, lookback=20, reversal_threshold=0.002):
    """
    Detect Liquidity Sweeps: Price dips below recent low/high then reverses.
    - Bullish sweep: New low < min(low_t-1 to t-lookback), then close > open + threshold
    - Bearish sweep: New high > max(high_t-1 to t-lookback), then close < open - threshold
    Returns df with 'liq_sweep_bull' and 'liq_sweep_bear' binary flags.
    """
    # Rolling min/max lows/highs
    rolling_min_low = df['low'].rolling(window=lookback).min().shift(1)
    rolling_max_high = df['high'].rolling(window=lookback).max().shift(1)
    
    # Bullish sweep: Break below min low, then reverse up
    break_below = df['low'] < rolling_min_low
    reversal_up = (df['close'] - df['open']) / df['open'] > reversal_threshold
    df['liq_sweep_bull'] = break_below & reversal_up
    
    # Bearish sweep: Break above max high, then reverse down
    break_above = df['high'] > rolling_max_high
    reversal_down = (df['open'] - df['close']) / df['open'] > reversal_threshold
    df['liq_sweep_bear'] = break_above & reversal_down
    
    return df

def detect_break_of_structure(df, lookback=20):
    """
    Detect Break of Structure (BOS): Price breaks recent high/low.
    - Bullish BOS: high > max(high_t-1 to t-lookback)
    - Bearish BOS: low < min(low_t-1 to t-lookback)
    Returns df with 'bos_bull' and 'bos_bear' binary flags.
    """
    # Rolling max high and min low (shifted to avoid lookahead)
    rolling_max_high = df['high'].rolling(window=lookback).max().shift(1)
    rolling_min_low = df['low'].rolling(window=lookback).min().shift(1)
    
    df['bos_bull'] = df['high'] > rolling_max_high
    df['bos_bear'] = df['low'] < rolling_min_low
    
    return df

def add_raw_features(df, lags=[1, 5, 10]):
    """
    Add raw price/volume features for ML (lags, returns, volatility).
    """
    # Price lags
    for lag in lags:
        df[f'close_lag_{lag}'] = df['close'].shift(lag)
        df[f'volume_lag_{lag}'] = df['volume'].shift(lag)
    
    # Log returns
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    
    # Volatility (std of returns)
    df['volatility_20'] = df['log_return'].rolling(window=20).std()
    
    return df

def add_smc_features(df):
    """
    Add all SMC features to the DataFrame.
    """
    logger.info("Adding SMC features...")
    df = detect_order_blocks(df)
    df = detect_fair_value_gaps(df)
    df = detect_liquidity_sweeps(df)
    df = detect_break_of_structure(df)
    logger.info("SMC features added.")
    return df

def main():
    """
    Main entry point: Load data, add features, save updated CSV.
    """
    input_file = 'btc_1m_mainnet_data.csv'
    output_file = 'btc_1m_with_features.csv'
    
    try:
        logger.info(f"Loading data from {input_file}...")
        df = pd.read_csv(input_file, parse_dates=['start_timestamp'], index_col='start_timestamp')
        
        # Add raw features
        df = add_raw_features(df)
        
        # Add SMC features
        df = add_smc_features(df)
        
        # Handle NaNs from shifts/rollings
        df.dropna(inplace=True)
        
        # Save updated DataFrame
        df.to_csv(output_file)
        logger.info(f"Saved DataFrame with {len(df.columns)} features ({len(df)} rows) to {output_file}")
        
        # Preview
        print(f"Preview of data with features:\n{df.head()}")
        print(f"Features: {df.columns.tolist()}")
        
    except FileNotFoundError:
        logger.error(f"{input_file} not found. Run fetch_data.py first.")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()