import pandas as pd

# 加载你在上一步清洗好的数据集
df = pd.read_csv('/Users/elviral/codeproject/capstone/data/filtered/cleaned_paper_dataset.csv')

print(f"原始 'cleaned_paper_dataset.csv' 形状: {df.shape}")

# --- 1. 重命名列 ---
print("正在重命名列以匹配论文...")
df = df.rename(columns={
    'symbol': 'Symbol',  # 确保 Symbol 列名一致
    'new_addresses': 'new_add',
    'active_addresses': 'active_add',
    'average_transaction_value': 'avg_trans_value',
    'volumeto': 'trading_volume',
    'market_cap_usd': 'size'
})

# --- 2. 处理缺失值 (NaN -> 0) ---
# 根据 describe() 中 min=0 的分析，将 NaN 填充为 0
print("正在填充 'raw features' 的缺失值 (NaN -> 0)...")
raw_features_cols = [
    'new_add', 'active_add', 'transaction_count', 'avg_trans_value',
    'trading_volume', 'volumefrom' # volumefrom 也一起处理
]
# 我们也应该处理 O-H-L-C 的缺失值，0 是一个合理填充
ohlc_cols = ['open', 'high', 'low', 'close'] 
df[raw_features_cols + ohlc_cols] = df[raw_features_cols + ohlc_cols].fillna(0)

# --- 3. 单位标准化 (USD -> Millions USD) ---
print("正在标准化 'trading_volume' 的单位 (除以 1,000,000)...")
# 在填充0之后再做除法，避免 0/1M 的问题
df['trading_volume'] = df['trading_volume'] / 1_000_000

# --- 4. 保存结果 ---
output_file = 'raw_dataset.csv'
df.to_csv(output_file, index=False)

print(f"\n阶段一完成！已保存到: {output_file}")
print("处理后数据预览:")
print(df[['Symbol', 'date', 'new_add', 'active_add', 'transaction_count', 'avg_trans_value', 'trading_volume', 'size']].head())
print("\n检查 'trading_volume' 的值 (应为百万单位):")
print(df['trading_volume'].describe())