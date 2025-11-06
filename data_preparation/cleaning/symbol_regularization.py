import pandas as pd

# === 配置 ===
INPUT_CSV = "C:/Users/Air/Documents/local_code/capstoneProject/capstone/data/test.csv"        # 改成你的原始CSV路径
SYMBOL_COL = "Symbol"              # 如果列名不同，这里改一下
SYMBOLS_OUT = "test.csv"        # 去重后的币种列表
SYMBOL_COUNTS_OUT = "test_counts.csv"  # 各币种出现次数

# 1) 读取
df = pd.read_csv(INPUT_CSV)

# 2) 清洗并获取唯一币种（去空值、去前后空格、统一大写）
s = (
    df[SYMBOL_COL]
    .dropna()
    .astype(str)
    .str.strip()
    .str.upper()
)

# 3) 唯一值 + 排序
symbols = sorted(pd.unique(s))

# 4) 统计总币种数
num_symbols = len(symbols)
print(f"Total unique symbols: {num_symbols}")

# 5) 保存去重后的列表到CSV（单列，按字母序）
pd.DataFrame({SYMBOL_COL: symbols}).to_csv(SYMBOLS_OUT, index=False)
print(f"Saved unique symbols to: {SYMBOLS_OUT}")

# 6)（可选）导出每个Symbol的出现次数，便于数据检查
symbol_counts = (
    s.value_counts()
     .rename_axis(SYMBOL_COL)
     .reset_index(name="rows")
     .sort_values(by=SYMBOL_COL)
)
symbol_counts.to_csv(SYMBOL_COUNTS_OUT, index=False)
print(f"Saved symbol counts to: {SYMBOL_COUNTS_OUT}")
