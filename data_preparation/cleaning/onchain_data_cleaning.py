import pandas as pd
import warnings

# 忽略 Pyt_on 警告（可选，用于清理输出）
warnings.filterwarnings('ignore')

# 定义文件名和时间范围
input_filename = './data/onchain_data.csv'
output_filename = 'cleaned_onchain_data.csv'
start_date = '2020-01-01'
end_date = '2024-12-30'

print(f"--- Processing {input_filename} ---")

try:
    # 1. 加载数据
    df_onchain = pd.read_csv(input_filename)
    print(f"Original shape: {df_onchain.shape}")
    print(df_onchain.head())

    # 2. 检查并转换 'date' 列
    if 'date' not in df_onchain.columns:
        raise KeyError("未找到 'date' 列。请检查CSV文件。")
    
    # 转换为 datetime 对象
    df_onchain['date'] = pd.to_datetime(df_onchain['date'])

    # 3. 筛选时间范围
    df_filtered = df_onchain[
        (df_onchain['date'] >= start_date) & 
        (df_onchain['date'] <= end_date)
    ].copy()
    
    print(f"\n--- Data after filtering for {start_date} to {end_date} ---")
    print(f"Filtered shape: {df_filtered.shape}")

    # 4. 检查并设置索引
    # 注意：根据之前的分析，列名是 'Symbol' (S大写)
    if 'Symbol' not in df_filtered.columns:
        # 如果 'Symbol' 不存在，尝试 'symbol'
        if 'symbol' in df_filtered.columns:
            df_filtered.rename(columns={'symbol': 'Symbol'}, inplace=True)
        else:
            raise KeyError("未找到 'Symbol' 或 'symbol' 列。请检查CSV文件。")

    df_processed = df_filtered.set_index(['Symbol', 'date'])
    
    # 5. 排序索引
    df_processed = df_processed.sort_index()

    print("\n--- Processed Data Head ---")
    print(df_processed.head())

    # 6. 保存处理后的数据
    df_processed.to_csv(output_filename)
    
    print(f"\nSuccessfully processed and saved data to {output_filename}")
    print(f"The data now contains records from {start_date} to {end_date}, indexed by 'Symbol' and 'date'.")

except FileNotFoundError:
    print(f"Error: {input_filename} not found.")
except KeyError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")