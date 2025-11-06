import pandas as pd
import numpy as np

# --- 1. Load the user-uploaded CSV file ---
try:
    df = pd.read_csv("./data/all_cryptos_ohlcv.csv")
    df.columns = df.columns.str.strip()

    # --- 2. Preprocessing steps (Convert date) ---
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # >>> 新增：时间范围筛选（含首尾，闭区间） <<<
    start = pd.Timestamp('2020-01-01')
    end   = pd.Timestamp('2024-12-30')

    before_rows = len(df)
    df = df[df['date'].between(start, end, inclusive='both')].copy()
    after_rows = len(df)
    print(f"[Date filter] kept rows: {after_rows} / {before_rows} "
          f"(removed {before_rows - after_rows}); "
          f"range: {start.date()} ~ {end.date()}")

    # （可选）如果你的 date 里带时区，先统一到UTC再 normalize：
    # df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_convert('UTC').dt.tz_localize(None)

    # --- 2.1 sort after clipping ---
    df = df.sort_values(by=['Symbol', 'date'])

    # --- 3. Statistics Before Cleaning (for logging) ---
    original_rows = len(df)
    unique_symbols_before = df['Symbol'].nunique()
    zero_close_rows = (df['close'] == 0).sum()

    print("--- Data Cleaning Started ---")
    print(f"Original total rows (after date clip): {original_rows}")
    print(f"Original unique symbols: {unique_symbols_before}")
    print(f"Rows with 'close' price == 0: {zero_close_rows}")

    # --- 4. Apply Cleaning Logic ---
    # 同时为 0 才删除：等价于 (close!=0) 或 (volumeto!=0)
    cleaned_df = df[(df['close'] != 0) | (df['volumeto'] != 0)].copy()

    # 可选：去重
    cleaned_df = cleaned_df.drop_duplicates(subset=['Symbol','date'])

    # --- 5. Statistics After Cleaning (for logging) ---
    cleaned_rows = len(cleaned_df)
    symbols_after_cleaning = cleaned_df['Symbol'].nunique()
    print(f"\n--- Data Cleaning Complete ---")
    print(f"Rows after cleaning: {cleaned_rows}")
    print(f"Total rows removed: {original_rows - cleaned_rows}")
    print(f"Symbols remaining: {symbols_after_cleaning}")

    # --- DEBUGGING ---
    print("\n--- DEBUGGING ---")
    print(f"type(cleaned_df) = {type(cleaned_df)}")
    print(f"has groupby? {'groupby' in dir(cleaned_df)}")
    print(f"columns: {list(cleaned_df.columns)}")
    print("--- END DEBUGGING ---\n")

    # --- 6. Recalculate Returns on Cleaned Data ---
    print("Attempting to calculate returns (pct_change)...")
    cleaned_df['close'] = pd.to_numeric(cleaned_df['close'], errors='coerce')
    cleaned_df['return'] = cleaned_df.groupby('Symbol', sort=False)['close'].pct_change()
    print("Successfully calculated returns.")
    cleaned_df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # --- 7. Set Index and Save ---
    cleaned_df = cleaned_df.set_index(['Symbol', 'date'])
    cleaned_df.to_csv("cleaned_ohlcv.csv")
    print("\nSuccessfully cleaned the data and saved it to 'cleaned_ohlcv.csv'.")

except Exception as e:
    print(f"An error occurred during data cleaning: {e}")
