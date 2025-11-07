# ipca_baseline.py

import pandas as pd
import numpy as np
from ipca import InstrumentedPCA

# ========= Step 1: 基本配置 =========

DATA_PATH = "/Users/elviral/codeproject/capstone/data/features_final.csv"  # 按你的项目结构改

SYMBOL_COL = "Symbol"
DATE_COL = "date"

# 用来构造 r_{i,t+1} 的基础收益列（当前是 t 的超额收益）
RET_COL = "asset_excess_return"

# 21 个特征（characteristics）列名（已按你的 CSV 写好）
CHAR_COLS = [
    "new_add",
    "active_add",
    "transaction_count",
    "avg_trans_value",
    "trading_volume",
    "size",
    "rev_log",
    "mom_7",
    "mom_14",
    "mom_21",
    "mom_30",
    "VaR5_90",
    "ES5_90",
    "capm_alpha_30",
    "capm_beta_30",
    "idio_vol_30",
    "rvol_yz_30",
    "illiq",
    "vol_shock_15",
    "vol_shock_30",
    "bid-ask",
]

# 要尝试的因子个数 K（可以和论文设定对比）
# N_FACTORS_LIST = [2, 3, 4]
N_FACTORS_LIST = [1]



# 是否按截面(z-score)标准化特征（常规 IPCA 做法，建议 True）
STANDARDIZE_CHARS = True

def show_obs_rate(X):
    """
    基于当前用于 IPCA 的 X，统计每个资产的观测次数和观测率。
    观测率 = 该资产的观测期数 / 总时间长度T
    """
    # MultiIndex: (id, t)
    ids = X.index.get_level_values("id")
    ts = X.index.get_level_values("t")

    total_T = ts.nunique()  # 整个样本的时间长度 T
    counts = X.groupby("id").size()  # 每个 id 有多少条观测
    obs_rate = counts / total_T

    # 还原 id -> symbol 映射，要和 make_panel_index 里的规则一致
    raw = pd.read_csv(DATA_PATH)
    sym_sorted = sorted(raw[SYMBOL_COL].unique())
    id_to_symbol = {i + 1: s for i, s in enumerate(sym_sorted)}

    summary = pd.DataFrame({
        "id": counts.index,
        "symbol": [id_to_symbol[i] for i in counts.index],
        "obs": counts.values,
        "obs_rate": obs_rate.values,
    })

    # 按观测率从高到低排，方便看哪些币完整，哪些很稀疏
    summary = summary.sort_values("obs_rate", ascending=False)

    print("\n===== Per-asset observation rate (based on current X) =====")
    print(summary.head(50).to_string(index=False))
    print("===========================================================")

    return summary



# step2 读取数据进行检查
def load_data():
    print(f"[Step 2] Loading data from {DATA_PATH} ...")
    df = pd.read_csv(DATA_PATH)

    # 基本列检查
    required = [SYMBOL_COL, DATE_COL, RET_COL] + CHAR_COLS
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # 日期类型 & 排序
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    df = df.sort_values([SYMBOL_COL, DATE_COL])

    print(f"Loaded {len(df)} rows, {df[SYMBOL_COL].nunique()} unique symbols.")
    return df




# Step 3：构造面板索引 (id, t)
def make_panel_index(df: pd.DataFrame) -> pd.DataFrame:
    print("[Step 3] Building panel indices (id, t)...")

    # 数字化 Symbol -> id
    sym_to_id = {sym: i + 1 for i, sym in enumerate(sorted(df[SYMBOL_COL].unique()))}
    df["id"] = df[SYMBOL_COL].map(sym_to_id)

    # 构造有序时间索引 t（dense rank）
    df = df.sort_values(DATE_COL)
    df["t"] = df[DATE_COL].rank(method="dense").astype(int)

    # 用 (id, t) 作为 MultiIndex，符合 ipca 文档 panel 模式要求 :contentReference[oaicite:0]{index=0}
    df = df.sort_values(["id", "t"]).set_index(["id", "t"])

    return df


# Step 4：构造 y = r_{i,t+1} 和 X = z_{i,t}
def build_y_X(df: pd.DataFrame):
    print("[Step 4] Building y = r_{i,t+1} and X = characteristics at t ...")

    # y: 对每个资产，把 asset_excess_return 往上平移一格，得到 r_{i,t+1}
    df["y"] = df.groupby(level="id")[RET_COL].shift(-1)

    # X: 当前期的 21 个特征
    X = df[CHAR_COLS]

    # 删除没有未来收益的行（每个币最后一天）
    mask = df["y"].notna()
    y = df.loc[mask, "y"]
    X = X.loc[mask]

    print(f"After aligning to r_(t+1): {len(y)} observations remain.")
    return y, X

def impute_chars_by_time(X: pd.DataFrame) -> pd.DataFrame:
    # 对每个 t 的截面，用中位数填充该期缺失
    X_imputed = X.copy()
    ts = X_imputed.index.get_level_values("t")

    for t in np.unique(ts):
        idx = (ts == t)
        X_t = X_imputed[idx]

        # 对每列：用当期截面中位数填补 NaN
        med = X_t.median(skipna=True)
        X_imputed.loc[idx] = X_t.fillna(med)

    return X_imputed


# Step 5：（推荐）按时间做截面标准化

def standardize_chars_by_time(X: pd.DataFrame) -> pd.DataFrame:
    print("[Step 5] Cross-sectional standardization of characteristics...")
    # MultiIndex: (id, t)
    ids = X.index.get_level_values(0)
    ts = X.index.get_level_values(1)

    X_std = X.copy()

    unique_t = np.unique(ts)
    for t in unique_t:
        idx = (ts == t)
        X_t = X_std[idx]
        if X_t.shape[0] <= 1:
            continue

        mean = X_t.mean()
        std = X_t.std(ddof=0).replace(0, 1.0)
        X_std[idx] = (X_t - mean) / std

    return X_std


def prepare_data():
    df = load_data()
    df = make_panel_index(df)
    y, X = build_y_X(df)

    # 1) 先对特征按时间截面填充缺失
    X = impute_chars_by_time(X)

    # 2) 再做截面标准化
    if STANDARDIZE_CHARS:
        X = standardize_chars_by_time(X)
        print("Standardization applied.")

    # 3) 现在只需要丢掉 y 是 NaN 的行即可
    valid = ~y.isna()
    X, y = X.loc[valid], y.loc[valid]

    # 打印有效资产数量
    valid_ids = X.index.get_level_values("id").unique()
    print(f"\n有效参与 IPCA 的币种数量: {len(valid_ids)}")
    print("示例币种:", list(
        df.reset_index()[["id", SYMBOL_COL]].drop_duplicates("id")
          .set_index("id")[SYMBOL_COL].loc[valid_ids][:15]
    ))
    print(f"Final panel for IPCA: {len(y)} obs, {X.shape[1]} characteristics.")
    return y, X




# Step 6：拟合 IPCA baseline（多个 K）
import pandas as pd

def fit_ipca_models(y: pd.Series, X: pd.DataFrame):
    results = {}

    for K in N_FACTORS_LIST:
        print(f"\n[Step 6] Fitting IPCA with K = {K} factors ...")

        regr = InstrumentedPCA(
            n_factors=K,
            intercept=False,
            max_iter=10000,
            iter_tol=1e-5,
        )

        regr = regr.fit(X=X, y=y, data_type="panel")

        Gamma, Factors = regr.get_factors(label_ind=True)

        # 预测：返回 np.array，长度与 X / y 一致
        y_hat_arr = regr.predict(X=X, data_type="panel")
        # 强制包装成 Series，并用 y 的 index 对齐
        y_hat = pd.Series(y_hat_arr.reshape(-1), index=y.index)

        results[K] = {
            "model": regr,
            "Gamma": Gamma,
            "Factors": Factors,
            "y_hat": y_hat,
        }

        print(f"IPCA(K={K}) finished: Gamma shape={Gamma.shape}, Factors shape={Factors.shape}")

    return results


# Step 7：计算简单版总 R² & 导出结果
def compute_total_R2(y_true: pd.Series, y_pred_like) -> float:
    """
    y_true: pandas Series, MultiIndex (id,t)
    y_pred_like: Series 或 DataFrame 或 ndarray，与 y_true 同顺序/长度
    """
    # 统一成 Series
    if isinstance(y_pred_like, pd.DataFrame):
        if y_pred_like.shape[1] != 1:
            raise ValueError("y_pred DataFrame has more than 1 column.")
        y_pred = y_pred_like.iloc[:, 0]
    elif isinstance(y_pred_like, pd.Series):
        y_pred = y_pred_like
    else:
        y_pred = pd.Series(np.asarray(y_pred_like).reshape(-1), index=y_true.index)

    # index 对齐，去掉 NaN
    y_true_aligned, y_pred_aligned = y_true.align(y_pred, join="inner")
    mask = (~y_true_aligned.isna()) & (~y_pred_aligned.isna())

    yt = y_true_aligned[mask].to_numpy()
    yp = y_pred_aligned[mask].to_numpy()

    if len(yt) == 0:
        return np.nan

    sst = np.sum((yt - yt.mean()) ** 2)
    ssr = np.sum((yt - yp) ** 2)

    return 1.0 - ssr / sst if sst > 0 else np.nan


def evaluate_results(y: pd.Series, results: dict):
    print("\n[Step 7] Evaluating models & saving outputs...")
    metrics = []

    for K, res in results.items():
        y_hat = res["y_hat"]
        R2_total = compute_total_R2(y, y_hat)

        print(f"K = {K}: In-sample Total R^2 = {R2_total:.6f}")
        metrics.append({"K": K, "R2_total_in_sample": R2_total})

        res["Gamma"].to_csv(f"ipca_Gamma_K{K}.csv")
        res["Factors"].to_csv(f"ipca_Factors_K{K}.csv")

    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_csv("ipca_metrics_summary.csv", index=False)
    print("Saved: ipca_metrics_summary.csv, ipca_Gamma_K*.csv, ipca_Factors_K*.csv")

    return metrics_df


def main():
    y, X = prepare_data()
    # 在 prepare_data() 之后加
    
    show_obs_rate(X)
    results = fit_ipca_models(y, X)
    _ = evaluate_results(y, results)

if __name__ == "__main__":
    main()
