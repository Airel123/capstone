import pandas as pd
import numpy as np

# 加载你在阶段二完成的数据集
input_file = '/Users/elviral/codeproject/capstone/data/factor_buiding/features_base_added.csv'
output_file = 'features_group2.csv'


print(f"正在加载: {input_file}...")
df = pd.read_csv(input_file)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by=['Symbol', 'date']).reset_index(drop=True)

print("正在计算 组2 特征 (rev 和 mom)...")

# --- 1) 短期反转：上一日回报 ---
# 推荐：简单收益版（更常见做法）
if 'simple_return' in df.columns:
    df['rev'] = df.groupby('Symbol')['simple_return'].shift(1)
else:
    # 兜底：若没有 simple_return，就用 log_return 的上一期
    df['rev'] = df.groupby('Symbol')['log_return'].shift(1)
#（可选）同时保留 log 口径，便于对比
df['rev_log'] = df.groupby('Symbol')['log_return'].shift(1)

# --- 2) 动量：L 日累计对数收益（跳过当天，包含 t-1）---
# 正确实现：对 log_return 先 shift(1)，再 rolling(sum)
momentum_windows = [7, 14, 21, 30]
df['log_return_lag1'] = df.groupby('Symbol')['log_return'].shift(1)

for L in momentum_windows:
    col = f'mom_{L}'
    print(f"  > 正在计算 {col}...")
    df[col] = (
        df.groupby('Symbol')['log_return_lag1']
          .transform(lambda s: s.rolling(window=L, min_periods=L).sum())
    )
    # 说明：这是“previous L days cumulative log return, skipping today”
    # 兼容论文“skip one day”的设定。:contentReference[oaicite:1]{index=1}

# 清理中间列
df = df.drop(columns=['log_return_lag1'])

# 保存
df.to_csv(output_file, index=False)
print(f"\n阶段三 (组2) 完成！已保存到: {output_file}")

# 简要预览
preview_cols = ['Symbol', 'date', 'log_return', 'rev', 'mom_7', 'mom_30']
print("\n新特征预览：")
print(df[preview_cols].dropna().head())
