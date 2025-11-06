import pandas as pd
import numpy as np
import sys

# --- 1. 加载和准备数据 ---
input_file = 'features_03_group3_final.csv' 
output_file = 'features_04_final.csv'

print(f"正在加载: {input_file}...")
try:
    df = pd.read_csv(input_file)
except FileNotFoundError:
    print(f"错误：文件 '{input_file}' 未找到。", file=sys.stderr)
    exit()

df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by=['Symbol', 'date'])

print("正在计算 [修正后的] 组4 特征 (illiq, bid-ask, vol shock)...")

# --- 2. 计算 illiq (Amihud Illiquidity) [已修正] ---
# [修正]：使用 simple_return
# 依赖: simple_return, trading_volume (已在阶段一转换为百万美元)
print("  > 正在计算 illiq (使用 simple_return)...")
df['illiq'] = df['simple_return'].abs() / df['trading_volume']

# 处理除以 0 (trading_volume=0) 导致的 inf
df['illiq'] = df['illiq'].replace([np.inf, -np.inf], np.nan)

# --- 3. 计算 Volatility Shock (vol shock) [已确认健壮性] ---
print("  > 正在计算 vol_shock_15 和 vol_shock_30...")
# [已确认]：.replace(0, np.nan) 确保了 log(0) 的健壮性
log_vol = np.log(df['trading_volume'].replace(0, np.nan))

# 逻辑: log(volume) - rolling_mean(log(volume))
df['vol_shock_15'] = log_vol - log_vol.groupby(df['Symbol']) \
                                       .rolling(window=15, min_periods=10) \
                                       .mean() \
                                       .reset_index(0, drop=True)

df['vol_shock_30'] = log_vol - log_vol.groupby(df['Symbol']) \
                                       .rolling(window=30, min_periods=20) \
                                       .mean() \
                                       .reset_index(0, drop=True)

# --- 4. 计算 bid-ask (Synthetic Spread) [已修正并增强健壮性] ---
print("  > 正在计算 bid-ask (增强健壮性)... (此步骤可能需要几分钟)")

def calculate_synthetic_spreads_robust(group):
    """
    [已修正] 为单个 Symbol (group) 计算 C-S 和 A-R 价差。
    """
    # [已确认] 1. 价格 > 0 才能做 log
    h = group['high'].replace(0, np.nan)
    l = group['low'].replace(0, np.nan)
    c = group['close'].replace(0, np.nan)
    
    # --- Corwin-Schultz (2012) Estimator ---
    ln_hl_ratio = np.log(h / l)
    beta = (ln_hl_ratio**2).rolling(window=2, min_periods=2).sum()
    gamma = (np.log(h.rolling(window=2).max() / l.rolling(window=2).min()))**2
    
    alpha_denom = 3 - 2 * np.sqrt(2)
    alpha = (np.sqrt(2 * beta) - np.sqrt(beta)) / alpha_denom - np.sqrt(gamma / alpha_denom)
    
    spread_cs = 2 * (np.exp(alpha) - 1) / (1 + np.exp(alpha))
    
    # --- Abdi-Ranaldo (2017) Estimator ---
    pt = (h + l) / 2      # Mid-price
    ct_1 = c.shift(1)    # Lagged close
    pt_1 = pt.shift(1)   # Lagged mid-price
    h_1 = h.shift(1)     # Lagged high
    l_1 = l.shift(1)     # Lagged low
    
    eta = 4 * (pt - ct_1) * (pt - pt_1) - (h - l)**2 - (h_1 - l_1)**2
    
    # [已确认] 2. 保护 eta < 0
    spread_ar = np.sqrt(np.maximum(0, eta)) / ct_1
    
    # [修正] 3. 剪裁异常值
    # 价差不应为负，且很少 > 100%
    spread_cs = spread_cs.clip(0, 1)
    spread_ar = spread_ar.clip(0, 1)
    
    # --- 最终特征 ---
    return (spread_cs + spread_ar) / 2

# 使用 groupby().apply() 应用函数
bid_ask_series = df.groupby('Symbol').apply(calculate_synthetic_spreads_robust)

# 将 MultiIndex Series 合并回 DataFrame
df['bid-ask'] = bid_ask_series.reset_index(level=0, drop=True)

# --- 5. 保存最终结果 ---
df.to_csv(output_file, index=False)

print("\n--- [修正后] 组4 计算完成 ---")
print(f"全部 21+ 个特征已计算完毕！")
print(f"最终数据集已保存到: {output_file}")

# 显示新特征的预览
print("\n新特征预览 (illiq, bid-ask, vol_shock_30):")
cols_to_show = ['Symbol', 'date', 'simple_return', 'illiq', 'bid-ask', 'vol_shock_30']
print(df[cols_to_show].dropna().head())