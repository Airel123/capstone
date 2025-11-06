import pandas as pd
import numpy as np

ROLLING_WINDOW_RVOL = 30
MIN_OBS_RVOL = 20

input_file = 'features_03_group3_b.csv'
output_file = 'features_03_group3_final.csv'

print(f"正在加载: {input_file}...")
df = pd.read_csv(input_file)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['Symbol','date']).reset_index(drop=True)

print(f"正在计算 rvol (Yang–Zhang, {ROLLING_WINDOW_RVOL}天滚动)...")

# 1) 准备 prev_close
df['prev_close'] = df.groupby('Symbol')['close'].shift(1)

# 2) 清洗非法价格（<=0 -> NaN），避免 log 非法
for c in ['open','high','low','close','prev_close']:
    df[c] = pd.to_numeric(df[c], errors='coerce')
    df.loc[df[c] <= 0, c] = np.nan
# 3) 向量化计算 YZ 的三个组件 —— 先写回到 df，保证有列名
df['ln_o'] = np.log(df['open'] / df['prev_close'])   # overnight: o_t / c_{t-1}
df['ln_c'] = np.log(df['close'] / df['open'])        # open -> close
df['ln_h'] = np.log(df['high'] / df['open'])         # high / open
df['ln_l'] = np.log(df['low']  / df['open'])         # low  / open
df['rs_comp'] = df['ln_h'] * (df['ln_h'] - df['ln_c']) + df['ln_l'] * (df['ln_l'] - df['ln_c'])

# 4) 按 Symbol 做滚动统计
g = df.groupby('Symbol', group_keys=False)

# 样本方差（ddof=1）
var_o  = g['ln_o'].apply(lambda s: s.rolling(ROLLING_WINDOW_RVOL, min_periods=MIN_OBS_RVOL).var(ddof=1))
var_c  = g['ln_c'].apply(lambda s: s.rolling(ROLLING_WINDOW_RVOL, min_periods=MIN_OBS_RVOL).var(ddof=1))
# Rogers–Satchell 组件窗口均值
var_rs = g['rs_comp'].apply(lambda s: s.rolling(ROLLING_WINDOW_RVOL, min_periods=MIN_OBS_RVOL).mean())

# 5) 有效样本数 n，用来计算 k(n)
n_eff = g['ln_o'].apply(lambda s: s.rolling(ROLLING_WINDOW_RVOL, min_periods=MIN_OBS_RVOL).count())
with np.errstate(divide='ignore', invalid='ignore'):
    k = 0.34 / (1.34 + (n_eff + 1) / (n_eff - 1))

# 6) 组合得到 YZ 方差并开方
total_var = var_o + k * var_c + (1 - k) * var_rs
total_var = total_var.where(total_var >= 0)  # 负值置 NaN 以防数值噪声
df['rvol_yz_30'] = np.sqrt(total_var)

# 7) 清理与保存（可保留中间列进行调试，稳定后再删除）
df = df.drop(columns=['prev_close', 'ln_o','ln_c','ln_h','ln_l','rs_comp'], errors='ignore')
df.to_csv(output_file, index=False)
print("✅ rvol 完成 ->", output_file)
print(df[['Symbol','date','rvol_yz_30']].dropna().head())
