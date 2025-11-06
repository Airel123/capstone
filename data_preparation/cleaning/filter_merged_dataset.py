# filter_merged_dataset.py
import pandas as pd
from datetime import datetime

# ===== 配置 =====
INPUT = "/Users/elviral/codeproject/capstone/data/merge/merged_full_dataset.csv"
OUTPUT = "filtered_merged_full_dataset.csv"
REMOVED = "filtered_removed_rows.csv"

DATE_START = "2020-01-01"
DATE_END   = "2024-12-30"   # 含当天

STABLECOINS = {
    "USDT","USDC","BUSD","DAI","TUSD","PAX","GUSD","LUSD","SUSD","HUSD","FEI","FRAX","USTC"
}

pd.set_option("display.width", 180)
pd.set_option("display.max_columns", 200)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[ \t/\\]+", "_", regex=True)
          .str.replace(r"[^0-9a-zA-Z_]+", "", regex=True)
    )
    return df

def to_num(s):
    # 把字符串市值转成数字：去逗号与空白，无法转换变 NaN
    return pd.to_numeric(
        pd.Series(s, dtype="string").str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )

def main():
    # 1) 读取
    df = pd.read_csv(INPUT)
    print(f"✅ 读取完成：{INPUT}  shape={df.shape}")

    # 2) 规范列名、关键列
    df = normalize_columns(df)
    if "symbol" not in df.columns or "date" not in df.columns:
        raise ValueError("缺少必要列 'symbol' 或 'date'。")

    # 3) 统一 symbol、date
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()

    # 4) 市值列处理
    if "market_cap_usd" not in df.columns:
        raise ValueError("缺少市值列 'market_cap_usd'。请确认输入文件包含该列。")
    df["market_cap_usd"] = to_num(df["market_cap_usd"])

    # 5) 过滤前统计
    total_before = len(df)
    uniq_sym_before = df["symbol"].nunique()
    print(f"总行数(过滤前): {total_before}，唯一Symbol数: {uniq_sym_before}")
    print("日期范围(原始):", df["date"].min(), "~", df["date"].max())

    # 6) 时间范围过滤（含首尾）
    start = pd.Timestamp(DATE_START)
    end   = pd.Timestamp(DATE_END)
    mask_date = (df["date"] >= start) & (df["date"] <= end)
    df_date = df.loc[mask_date].copy()
    print(f"\n⏱ 时间窗过滤: [{DATE_START} ~ {DATE_END}] 保留 {len(df_date)} 行（剔除 {total_before - len(df_date)} 行）")

    # 7) 稳定币剔除
    stables = {s.upper() for s in STABLECOINS}
    is_stable = df_date["symbol"].isin(stables)
    stable_rows = df_date.loc[is_stable]
    df_no_stable = df_date.loc[~is_stable].copy()
    print(f"💱 稳定币剔除: {stable_rows['symbol'].nunique()} 个稳定币，{len(stable_rows)} 行被删除")
    if not stable_rows.empty:
        print("被剔除的稳定币TOP(10):")
        print(stable_rows["symbol"].value_counts().head(10))

    # 8) 市值过滤：仅保留 market_cap_usd >= 1e8（NaN 视为不满足条件而剔除）
    threshold = 100_000_000
    mask_mcap = df_no_stable["market_cap_usd"] >= threshold
    keep_mcap = df_no_stable.loc[mask_mcap].copy()
    drop_mcap = df_no_stable.loc[~mask_mcap].copy()

    print(f"\n🏦 市值过滤: >= {threshold:,} USD")
    print(f"保留 {len(keep_mcap)} 行；因市值不足或缺失剔除 {len(drop_mcap)} 行")
    # 可选：看看缺失市值的占比
    na_mcap = df_no_stable["market_cap_usd"].isna().sum()
    print(f"其中市值缺失(NA)行数: {na_mcap}")

    # 9) 汇总剔除项并导出
    removed_all = pd.concat([stable_rows, drop_mcap], axis=0, ignore_index=True)
    removed_all = removed_all.sort_values(["symbol", "date"])
    removed_all.to_csv(REMOVED, index=False)
    print(f"\n🗂️ 已导出被剔除的行: {REMOVED}  shape={removed_all.shape}")

    # 10) 最终结果导出
    result = keep_mcap.sort_values(["symbol", "date"])
    result.to_csv(OUTPUT, index=False)
    print(f"✅ 已导出过滤后结果: {OUTPUT}  shape={result.shape}")

    # 11) 过滤后统计
    print("\n—— 过滤后统计 ——")
    print("唯一Symbol数:", result["symbol"].nunique())
    print("日期范围:", result["date"].min(), "~", result["date"].max())

    # 12) 简要预览
    print("\n[结果示例 .head(5)]")
    print(result.head(5))

if __name__ == "__main__":
    main()
