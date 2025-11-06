import pandas as pd
import numpy as np

# ========================
# 0. 读取原始数据
# ========================
print("正在加载原始数据 ...")
SRC_PATH = "/Users/elviral/codeproject/capstone/data/raw_dataset.csv"
try:
    df = pd.read_csv(SRC_PATH)
except FileNotFoundError:
    print(f"错误：未找到 {SRC_PATH}")
    raise

# 去重 + 排序，防止乱序/重复导致 shift 错位
df = (
    df.drop_duplicates(subset=['Symbol', 'date'])
      .sort_values(['Symbol', 'date'])
      .reset_index(drop=True)
)

# 关键字段存在性检查
required_cols = {'Symbol', 'date', 'close', 'size'}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"缺少必要列: {missing}. 需要包含 {required_cols}")

# 日期转 datetime
df['date'] = pd.to_datetime(df['date'])

# ========================
# 1. 收益率（log / simple）
# ========================
print("计算 log_return / simple_return ...")
# 防止 log(0)、/0
df['close'] = df['close'].replace(0, np.nan)

# 对数收益：log(close)_t - log(close)_{t-1}
# 使用 transform(np.log).diff() 数值更稳定
df['log_return'] = df.groupby('Symbol')['close'].transform(np.log).diff()

# 简单收益：(P_t / P_{t-1}) - 1
df['simple_return'] = df['close'] / df.groupby('Symbol')['close'].shift(1) - 1

# ========================
# 2. 无风险利率（DGS1MO）
# ========================
print("加载并合并无风险利率 (DGS1MO) ...")
RF_PATH = "/Users/elviral/codeproject/capstone/data/factor_buiding/DGS1MO.csv"
try:
    rf_df = pd.read_csv(RF_PATH)
except FileNotFoundError:
    print("错误：请先从 FRED 下载 DGS1MO.csv (https://fred.stlouisfed.org/series/DGS1MO)")
    raise

# 统一日期 + 列名
if 'DATE' not in rf_df.columns or 'DGS1MO' not in rf_df.columns:
    raise ValueError("DGS1MO.csv 需要包含列：DATE, DGS1MO")
rf_df['date'] = pd.to_datetime(rf_df['DATE'])
rf_df = rf_df.rename(columns={'DGS1MO': 'risk_free_annual'})[['date', 'risk_free_annual']]

# '.' -> NaN，再转数值
rf_df['risk_free_annual'] = pd.to_numeric(rf_df['risk_free_annual'], errors='coerce')

# 将 rf 对齐到交易日历再 ffill（避免周末/节假日缺口）
all_days = pd.DataFrame({'date': sorted(df['date'].unique())})
rf_df = (
    all_days.merge(rf_df, on='date', how='left')
            .sort_values('date')
            .ffill()
)

# 合并 + 年化百分比转日度小数： (1 + R_annual/100)^(1/365) - 1
df = df.merge(rf_df, on='date', how='left')
df['risk_free_rate'] = (1.0 + (df['risk_free_annual'] / 100.0)) ** (1/365.0) - 1.0

# ========================
# 3. 市场指数（价值加权, t-1 权重）
# ========================
print("构建价值加权市场指数 (Market_Return) ...")
# t-1 市值（你的 size 已是“总市值”）
df['size_lagged'] = df.groupby('Symbol')['size'].shift(1)

# 当日 t 对应的 “t-1 总市值”
df['total_market_cap_daily_lagged'] = df.groupby('date')['size_lagged'].transform('sum')

# t-1 权重
df['weight_lagged'] = df['size_lagged'] / df['total_market_cap_daily_lagged']

# 保护：若当日 t-1 市值总和为 0 或 NaN，屏蔽该日的权重/贡献，避免伪 0
mask_zero = (df['total_market_cap_daily_lagged'].isna()) | (df['total_market_cap_daily_lagged'] <= 0)
df.loc[mask_zero, ['weight_lagged']] = np.nan

# 加权“简单收益”贡献
df['weighted_simple_return'] = df['weight_lagged'] * df['simple_return']

# 当日聚合（若全 NaN -> NaN，而不是 0）
mkt = (
    df.groupby('date')['weighted_simple_return']
      .sum(min_count=1)  # 关键：全 NaN 返回 NaN
      .rename('Market_Return')
      .reset_index()
)
df = df.merge(mkt, on='date', how='left')

# ========================
# 4. 超额收益（简单收益口径）
# ========================
print("计算超额收益 (Excess Returns) ...")
df['asset_excess_return']  = df['simple_return']  - df['risk_free_rate']
df['market_excess_return'] = df['Market_Return'] - df['risk_free_rate']

# ========================
# 5. 导出（清理中间列）
# ========================
OUTPUT = "features_base_added.csv"
cols_to_drop = [
    'risk_free_annual', 'size_lagged',
    'total_market_cap_daily_lagged', 'weight_lagged',
    'weighted_simple_return'
]
df_final = df.drop(columns=cols_to_drop, errors='ignore')

df_final.to_csv(OUTPUT, index=False)

print("\n✅ 阶段二完成！已保存到:", OUTPUT)
print("预览：")
print(
    df_final[['Symbol','date','log_return','simple_return',
              'risk_free_rate','Market_Return',
              'asset_excess_return','market_excess_return']].head(10)
)
