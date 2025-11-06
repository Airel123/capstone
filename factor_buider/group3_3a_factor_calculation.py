import pandas as pd
import numpy as np

# 定义滚动窗口
ROLLING_WINDOW_RISK = 90
MIN_OBS_RISK = 75 # 设定一个合理的最小观测期 (例如 ~80% of window)

# --- 1. 加载数据 ---
# [!] 请确保这里的文件名是你上一步 [修正后组2] 的输出
input_file = 'features_group2.csv' 
output_file = 'features_03_group3_a.csv'

print(f"正在加载: {input_file}...")
try:
    df = pd.read_csv(input_file)
except FileNotFoundError:
    print(f"错误：文件 {input_file} 未找到。")
    exit()

df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by=['Symbol', 'date'])

print(f"正在计算 VaR 和 ES (90天滚动窗口) [已修正]...")

# --- 2. 计算 VaR (5%) [已修正] ---
# 论文描述: "5% historical value-at-risk... based on 90 days of realised returns"
# [修正]：重命名为 VaR5_90
print("  > 正在计算 VaR5_90...")
df['VaR5_90'] = df.groupby('Symbol')['log_return'] \
                   .rolling(window=ROLLING_WINDOW_RISK, min_periods=MIN_OBS_RISK) \
                   .quantile(0.05, interpolation='linear') \
                   .reset_index(0, drop=True)

# --- 3. 计算 ES (5%) [已修正] ---
# 论文描述: "...and corresponding expected shortfall"

def calculate_es_5_revised(window_data):
    """
    [已修正]
    计算给定窗口数据的 5% 预期短缺 (Expected Shortfall)。
    window_data 是一个 numpy 数组 (因为 raw=True)。
    """
    
    # 1. [修正]：使用 nanquantile 来计算 VaR，并显式指定 'linear' 方法
    # 这确保了与 pandas.rolling().quantile() 的 'linear' 插值法一致
    try:
        var_5 = np.nanquantile(window_data, 0.05, method='linear')
    except Exception:
        # 如果窗口内全是 NaN，nanquantile 会报错
        return np.nan

    if np.isnan(var_5):
        return np.nan

    # 2. [修正]：使用 <= (小于或等于) 来筛选尾部
    shortfall_returns = window_data[window_data <= var_5]
    
    # 3. 移除 NaN 值后计算均值
    shortfall_returns = shortfall_returns[~np.isnan(shortfall_returns)]
    
    if shortfall_returns.size == 0:
        # 即使使用 <=，如果数据不足或特殊，仍可能为空
        return np.nan
    else:
        return np.mean(shortfall_returns)

# [修正]：重命名为 ES5_90
print("  > 正在计算 ES5_90...")
df['ES5_90'] = df.groupby('Symbol')['log_return'] \
                  .rolling(window=ROLLING_WINDOW_RISK, min_periods=MIN_OBS_RISK) \
                  .apply(calculate_es_5_revised, raw=True) \
                  .reset_index(0, drop=True)

# --- 4. 保存 ---
print(f"已保存到: {output_file}")
df.to_csv(output_file, index=False)

print("\n--- [修正后] VaR 和 ES 计算完成 ---")
print(df[['Symbol', 'date', 'log_return', 'VaR5_90', 'ES5_90']].dropna().head())