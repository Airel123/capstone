# merge_three_tables_verbose.py
import pandas as pd

pd.set_option("display.width", 180)
pd.set_option("display.max_columns", 200)

# ============== 小工具：统一打印信息 ==============
def show_info(df, name, key_cols=("symbol", "date"), head_n=5):
    print(f"\n{'='*30} [{name}] 基本信息 {'='*30}")
    print(f"shape: {df.shape}")
    print("columns:", list(df.columns))
    print("\ndtypes:\n", df.dtypes)
    if head_n:
        print(f"\n.head({head_n}):\n", df.head(head_n))
    # 关键列存在性与缺失
    for c in key_cols:
        print(f"- has column '{c}':", c in df.columns)
        if c in df.columns:
            miss = df[c].isna().sum()
            print(f"  missing {c}: {miss}")
    # 唯一 symbol 数、日期范围
    if "symbol" in df.columns:
        print("unique symbols:", df["symbol"].nunique())
        print("top symbols (value_counts head 10):\n", df["symbol"].value_counts().head(10))
    if "date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["date"]):
        print("date min/max:", df["date"].min(), " ~ ", df["date"].max())

def dedup_log(df, subset, name):
    before = len(df)
    df2 = df.drop_duplicates(subset=subset, keep="last")
    after = len(df2)
    if after != before:
        print(f"[{name}] 去重: {before} -> {after}（按 {subset}）")
    else:
        print(f"[{name}] 无需去重（按 {subset}）")
    return df2

# ============== 1) 读取 ==============
df1 = pd.read_csv("/Users/elviral/codeproject/capstone/data/merge1_onchain_data.csv")
df2 = pd.read_csv("/Users/elviral/codeproject/capstone/data/merge2_cryptocompare_ohlcv.csv")
df3 = pd.read_csv("/Users/elviral/codeproject/capstone/data/merge3_market_cap_data.csv")

print("✅ 已读取三个CSV文件。")

# ============== 2) 列名标准化 ==============
def normalize_columns(df):
    # 全小写、去首尾空格、空格与特殊字符替换为下划线
    cols = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[ \t/\\]+", "_", regex=True)
          .str.replace(r"[^0-9a-zA-Z_]+", "", regex=True)
    )
    df.columns = cols
    return df

df1 = normalize_columns(df1)
df2 = normalize_columns(df2)
df3 = normalize_columns(df3)

# df3 有列名 'data' -> 'date'
if "data" in df3.columns and "date" not in df3.columns:
    df3 = df3.rename(columns={"data": "date"})

# ============== 3) 统一 Symbol 大小写、Date 类型 ==============
for df, name in [(df1, "onchain"), (df2, "ohlcv"), (df3, "mcap")]:
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    else:
        raise ValueError(f"[{name}] 缺少 symbol 列。")

    if "date" in df.columns:
        # 常见格式例如: 2020/1/1；统一转换并去掉时间部分
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = df["date"].dt.normalize()
    else:
        raise ValueError(f"[{name}] 缺少 date 列（或需把 data 重命名为 date）。")

show_info(df1, "df1_onchain(统一列名后)")
show_info(df2, "df2_ohlcv(统一列名后)")
show_info(df3, "df3_mcap(统一列名后)")

# ============== 4) 关键列去重（按 symbol+date）===============
df1 = dedup_log(df1, subset=["symbol", "date"], name="df1_onchain")
df2 = dedup_log(df2, subset=["symbol", "date"], name="df2_ohlcv")
df3 = dedup_log(df3, subset=["symbol", "date"], name="df3_mcap")

# ============== 5) 合并（外连接，保留全部记录）=============
print("\n🧩 开始合并 df1 与 df2（outer on ['symbol', 'date']）...")
merged_12 = pd.merge(df1, df2, on=["symbol", "date"], how="outer", suffixes=("", "_ohlcvdup"))
show_info(merged_12, "merged_12")

# 检查潜在重复命名（如果两边都有同名列，pandas会加后缀）
dup_cols = [c for c in merged_12.columns if c.endswith("_ohlcvdup")]
if dup_cols:
    print("⚠️ 发现同名列重复（来自第二张表），保留原列，删除带后缀列：", dup_cols)
    merged_12 = merged_12.drop(columns=dup_cols)

print("\n🧩 将市值数据并入（outer on ['symbol', 'date']）...")
cols_keep_mcap = ["date", "symbol"]
for c in ["market_cap_usd", "rank", "name"]:
    if c in df3.columns:
        cols_keep_mcap.append(c)
    else:
        print(f"⚠️ df3 不含列: {c}")

merged = pd.merge(merged_12, df3[cols_keep_mcap], on=["symbol", "date"], how="outer")
show_info(merged, "merged(all three)")

# ============== 6) 基础质量检查与空值统计 ==============
print("\n📊 关键数值列空值统计（只挑选存在的列）")
num_candidates = [
    "open","high","low","close","volumefrom","volumeto",
    "market_cap_usd",
    "active_addre","average_transaction_value","new_addres","transaction_count","time"
]
present = [c for c in num_candidates if c in merged.columns]
print(merged[present].isna().sum().sort_values(ascending=False))

# ============== 7) 排序与导出 ==============
merged = merged.sort_values(["symbol", "date"])
merged.to_csv("merged_full_dataset.csv", index=False)
print("\n✅ 已导出：merged_full_dataset.csv（outer join 全量保留）")

# ============== 8) 可选：仅保留三表都出现过的严格交集（inner join） ==============
# 若需要“严格交集”，可进行以下步骤：
inner = pd.merge(
    pd.merge(df1[["symbol","date"]], df2[["symbol","date"]], on=["symbol","date"], how="inner"),
    df3[["symbol","date"]], on=["symbol","date"], how="inner"
)
merged_inner = pd.merge(merged, inner, on=["symbol","date"], how="inner")
merged_inner = merged_inner.sort_values(["symbol","date"])
merged_inner.to_csv("merged_strict_inner.csv", index=False)
print("✅ 已导出：merged_strict_inner.csv（仅三表都同时存在的记录）")

# ============== 9) （可选）时间范围筛选示例 ==============
# 如果你要限制到固定时间窗（例如 2020-01-01 至 2024-12-30），取消下面注释
# start, end = pd.Timestamp("2020-01-01"), pd.Timestamp("2024-12-30")
# win = merged[(merged["date"] >= start) & (merged["date"] <= end)].copy()
# win.to_csv("merged_full_dataset_20200101_20241230.csv", index=False)
# print("✅ 已导出：merged_full_dataset_20200101_20241230.csv（时间范围筛选后）")
