# inspect_merged_outputs.py
import os
import pandas as pd

pd.set_option("display.width", 180)
pd.set_option("display.max_columns", 200)

FILES = [
    # ("/Users/elviral/codeproject/capstone/data/filtered/cleaned_paper_dataset.csv", "Cleaned dataset"),
    ("/Users/elviral/codeproject/capstone/data/features_final.csv", "feature dataset"),
]

NUM_COL_CANDIDATES = [
    "open","high","low","close","volumefrom","volumeto",
    "market_cap_usd","rank","time",
    "active_addre","average_transaction_value","new_addres","transaction_count",
]

def show_info(df: pd.DataFrame, tag: str):
    print(f"\n{'='*30} [{tag}] 基本结构 {'='*30}")
    print("shape:", df.shape)
    print("columns:", list(df.columns))

    print("\n[dtypes]")
    print(df.dtypes)

    print("\n[示例 .head(5)]")
    print(df.head(5))

    print("checkpoint")
    symbol_counts = (
        df['Symbol']
        .value_counts()
        .rename_axis('Symbol')
        .reset_index(name='count')
    )
    print(symbol_counts)


    # 关键列检测
    for col in ["symbol", "date"]:
        print(f"\n- 是否包含列 '{col}':", col in df.columns)
        if col in df.columns:
            print(f"  缺失个数: {df[col].isna().sum()}")

    # 统一转为 datetime（仅展示范围，不会修改原数据）
    if "date" in df.columns:
        _date = pd.to_datetime(df["date"], errors="coerce")
        print("日期范围:", _date.min(), "~", _date.max())
    if "symbol" in df.columns:
        print("唯一 symbol 数:", df["symbol"].nunique())
        print("symbol 样例:", df["symbol"].dropna().astype(str).str.upper().value_counts().head(10))

    # 缺失值统计（仅展示候选列中存在的）
    exist_num_cols = [c for c in NUM_COL_CANDIDATES if c in df.columns]
    if exist_num_cols:
        print("\n[关键数值列缺失值统计]")
        print(df[exist_num_cols].isna().sum().sort_values(ascending=False))

    # 数值列基本统计
    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if num_cols:
        print("\n[数值列基本统计 .describe()]")
        print(df[num_cols].describe().T)

    # 重复键检查（symbol+date）
    if set(["symbol","date"]).issubset(df.columns):
        dup = df.duplicated(subset=["symbol","date"]).sum()
        print(f"\n[主键重复检测 symbol+date] 重复行数: {dup}")

def main():
    for path, label in FILES:
        print("\n" + "#"*90)
        print(f"检查文件: {path}  | 标签: {label}")
        if not os.path.exists(path):
            print("❌ 文件不存在，跳过。")
            continue
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"❌ 读取失败: {e}")
            continue
        show_info(df, f"{label} | {path}")

if __name__ == "__main__":
    main()
