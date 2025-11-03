import pandas as pd
import numpy as np

# --- 1. Load the user-uploaded CSV file ---
try:
    df = pd.read_csv("data preparation/data processing/all_cryptos_ohlcv.csv")

    # --- 2. Preprocessing steps (Convert date, sort) ---
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['Symbol', 'date'])

    # --- 3. Statistics Before Cleaning (for logging) ---
    original_rows = len(df)
    unique_symbols_before = df['Symbol'].nunique()
    zero_close_rows = len(df[df['close'] == 0])
    
    print("--- Data Cleaning Started ---")
    print(f"Original total rows: {original_rows}")
    print(f"Original unique symbols: {unique_symbols_before}")
    print(f"Rows with 'close' price == 0: {zero_close_rows}")

    # --- 4. Apply Cleaning Logic ---
    cleaned_df = df[(df['close'] != 0) | (df['volumeto'] != 0)].copy()

    # --- 5. Statistics After Cleaning (for logging) ---
    cleaned_rows = len(cleaned_df)
    symbols_after_cleaning = cleaned_df['Symbol'].nunique()
    
    print(f"\n--- Data Cleaning Complete ---")
    print(f"Rows after cleaning: {cleaned_rows}")
    print(f"Total rows removed: {original_rows - cleaned_rows}")
    print(f"Symbols remaining: {symbols_after_cleaning}")
    

    # --- [NEW] DEBUGGING BLOCK ---
    # We add this block right before the line that caused the error.
    print("\n--- DEBUGGING ---")
    try:
        print(f"Type of cleaned_df: {type(cleaned_df)}")
        print(f"Is 'groupby' an attribute of cleaned_df? {'groupby' in dir(cleaned_df)}")
        print(f"What is cleaned_df.groupby? {cleaned_df.groupby}")
    except Exception as e:
        print(f"An error occurred during debugging: {e}")
    print("--- END DEBUGGING ---\n")
    # --- END [NEW] DEBUGGING BLOCK ---


    # --- 6. Recalculate Returns on Cleaned Data ---
    # This is the line that caused the error in your screenshot
    print("Attempting to calculate returns (the line that failed)...")
    cleaned_df['return'] = cleaned_df.groupby('Symbol')['close'].pct_change()
    print("Successfully calculated returns.")
    
    # Handle potential infinite values
    cleaned_df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # --- 7. Set Index and Save ---
    cleaned_df = cleaned_df.set_index(['Symbol', 'date'])
    
    # Save the cleaned data to a new file
    cleaned_df.to_csv("cleaned_ohlcv.csv")
    
    print(f"\nSuccessfully cleaned the data and saved it to 'cleaned_ohlcv.csv'.")


except Exception as e:
    print(f"An error occurred during data cleaning: {e}")