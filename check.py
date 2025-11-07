import pandas as pd
import numpy as np

DATA_PATH = "/Users/elviral/codeproject/capstone/data/features_final.csv"
SYMBOL_COL = "Symbol"
RET_COL = "asset_excess_return"

def check_return_coverage():
    df_raw = pd.read_csv(DATA_PATH)
    # 看每个币 asset_excess_return 的非缺失比例
    cov = (
        df_raw
        .groupby(SYMBOL_COL)[RET_COL]
        .apply(lambda s: s.notna().mean())
        .sort_values(ascending=False)
    )

    print("\n===== 每个币 asset_excess_return 非 NaN 占比(前40) =====")
    print(cov.head(40))
    print("====================================================")



if __name__ == "__main__":
    check_return_coverage()

