import pandas as pd

# ===== 配置 =====
OHLCV_CSV   = "C:/Users/Air/Documents/local_code/capstoneProject/capstone/data/ohlcv_symbols.csv"    # 以此为基准对齐
ONCHAIN_CSV = "C:/Users/Air/Documents/local_code/capstoneProject/capstone/data/test_symbols.csv"
COL = "Symbol"                       # 两个CSV的列名（如不同请改）

# ===== 读取并标准化（去空、去重、去首尾空格、统一大写、排序）=====
def load_symbols(path, col):
    s = (pd.read_csv(path)[col]
         .dropna()
         .astype(str)
         .str.strip()
         .str.upper()
        )
    return pd.DataFrame({col: sorted(s.unique())})

ohlcv = load_symbols(OHLCV_CSV, COL)
onch  = load_symbols(ONCHAIN_CSV, COL)

# ===== 按 ohlcv 为基准做左连接，标记 onchain 中是否存在 =====
aligned = ohlcv.merge(
    onch.assign(in_onchain=True),
    on=COL, how="left"
)
aligned["in_onchain"] = aligned["in_onchain"].fillna(False)

# ===== 差异集 =====
both                 = aligned[aligned["in_onchain"]].copy()
missing_in_onchain   = aligned[~aligned["in_onchain"]].copy()  # ohlcv有、onchain缺
extras_in_onchain    = onch[~onch[COL].isin(ohlcv[COL])].copy()  # onchain有、ohlcv缺

# ===== 保存结果 =====
aligned.to_csv("aligned_symbols.csv", index=False)                # 全量对齐结果（含布尔标记）
both.to_csv("intersection_symbols.csv", index=False)              # 交集
missing_in_onchain.to_csv("missing_in_onchain.csv", index=False)  # 仅在 ohlcv
extras_in_onchain.to_csv("extras_in_onchain.csv", index=False)    # 仅在 onchain

# ===== 统计输出 =====
print("=== Summary ===")
print(f"OHLCV unique symbols:   {len(ohlcv)}")
print(f"Onchain unique symbols: {len(onch)}")
print(f"In both:                {len(both)}")
print(f"In OHLCV only:          {len(missing_in_onchain)}")
print(f"In Onchain only:        {len(extras_in_onchain)}")
print("Files saved: aligned_symbols.csv, intersection_symbols.csv, "
      "missing_in_onchain.csv, extras_in_onchain.csv")
