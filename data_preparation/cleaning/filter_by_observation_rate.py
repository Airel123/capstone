# filter_by_observation_rate.py
import pandas as pd

# ===== 参数设置 =====
INPUT = "/Users/elviral/codeproject/capstone/filtered_merged_full_dataset.csv"
OUTPUT = "filtered_dataset_observed75.csv"
REMOVED = "filtered_dataset_removed_symbols.csv"

DATE_START = "2020-01-01"
DATE_END   = "2024-12-30"

pd.set_option("display.width", 180)
pd.set_option("display.max_columns", 200)

def main():
    # 1) 读取
    df = pd.read_csv(INPUT)
    print(f"✅ 读取完成：{INPUT}  shape={df.shape}")

    # 2) 日期和 Symbol 规范化
    df.columns = df.columns.str.strip().str.lower()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()

    # 3) 计算样本期天数
    date_range = pd.date_range(DATE_START, DATE_END, freq="D")
    total_days = len(date_range)
    print(f"📅 样本期：{DATE_START} ~ {DATE_END}，共 {total_days} 天")

    # 4) 每个 Symbol 的观测天数
    obs_counts = (
        df.groupby("symbol")["date"]
          .nunique()
          .reset_index()
          .rename(columns={"date": "observed_days"})
    )
    obs_counts["observation_rate"] = obs_counts["observed_days"] / total_days

    print("\n🔍 观测率统计前5行：")
    print(obs_counts.head())

    # 5) 找出低于 75% 的 Symbol
    threshold = 0.75
    low_obs = obs_counts[obs_counts["observation_rate"] < threshold]
    print(f"\n🚫 剔除观测率低于 {threshold*100:.0f}% 的 Symbol 数量：{len(low_obs)}")
    print("示例(前10个)：", low_obs["symbol"].head(10).tolist())

    # 6) 保留符合条件的 Symbol
    keep_symbols = set(obs_counts.loc[obs_counts["observation_rate"] >= threshold, "symbol"])
    df_filtered = df[df["symbol"].isin(keep_symbols)].copy()

    # 7) 导出结果
    df_filtered.to_csv(OUTPUT, index=False)
    low_obs.to_csv(REMOVED, index=False)

    print(f"\n✅ 已导出过滤结果：{OUTPUT}  shape={df_filtered.shape}")
    print(f"❎ 已导出被剔除的 Symbol 列表：{REMOVED}")
    print(f"过滤后唯一 Symbol 数: {df_filtered['symbol'].nunique()}")
    print(f"过滤后日期范围: {df_filtered['date'].min()} ~ {df_filtered['date'].max()}")

if __name__ == "__main__":
    main()
