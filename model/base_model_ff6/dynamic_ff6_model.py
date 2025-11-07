# dynamic_ff6_model.py

import numpy as np
import pandas as pd
from numpy.linalg import lstsq


def prepare_panel_for_dynamic_ff6(
    df: pd.DataFrame,
    factors: pd.DataFrame,
    z_cols,
    ret_col: str = "asset_excess_return",
    min_obs_asset: int = 20,
):
    """
    构造用于动态 FF6 的面板数据:
    - 使用 z_{i,t} 作为特征
    - 使用 f_{t+1} 作为因子
    - 使用 r_{i,t+1} 作为被解释变量 (前推收益)
    """

    factor_cols = ["MKT", "SIZE", "MOM", "LIQ", "VOL", "REV"]

    # 确保时间排序
    df = df.sort_values(["Symbol", "date"]).copy()

    # 1) 前推收益 r_{i,t+1}
    df["ret_fwd"] = (
        df.groupby("Symbol")[ret_col]
        .shift(-1)
    )

    # 2) 保留我们需要的列
    use_cols = ["Symbol", "date", "ret_fwd"] + list(z_cols)
    use = df[use_cols].copy()

    # 3) 因子使用 f_{t+1}: 对因子整体做 shift(-1)，再按 t merge
    #    对于日期 t，我们有 z_{i,t} 和 f_{t+1}，解释 r_{i,t+1}
    fwd_factors = factors.shift(-1).copy()
    fwd_factors = fwd_factors.rename(columns={c: c for c in factor_cols})

    # merge: 按 date，获取 f_{t+1}
    panel = use.merge(
        fwd_factors[factor_cols],
        left_on="date",
        right_index=True,
        how="inner",
    )

    # 4) 丢掉缺 z / f / ret_fwd 的观测
    panel = panel.dropna(subset=["ret_fwd"] + list(z_cols) + factor_cols)

    # 5) 过滤样本极少的资产（防止极端噪声币）
    if min_obs_asset is not None and min_obs_asset > 0:
        counts = panel.groupby("Symbol").size()
        good_symbols = counts[counts >= min_obs_asset].index
        panel = panel[panel["Symbol"].isin(good_symbols)]

    return panel, factor_cols


def run_dynamic_ff6(
    df: pd.DataFrame,
    factors: pd.DataFrame,
    z_cols,
    ret_col: str = "asset_excess_return",
    min_obs_asset: int = 20,
):
    """
    运行动态 FF6 (Panel B 风格)：

        r_{i,t+1} = (Gamma_beta @ z_{i,t})' f_{t+1} + eps

    通过 pooled OLS 拟合：
        r_{i,t+1} = (f_{t+1} ⊗ z_{i,t})' theta

    返回：
        R2_total_dynamic
        Gamma_beta: (K x L) matrix
        panel_with_pred: 包含实际与拟合收益的面板，用于后续分析
    """

    # 准备面板
    panel, factor_cols = prepare_panel_for_dynamic_ff6(
        df=df,
        factors=factors,
        z_cols=z_cols,
        ret_col=ret_col,
        min_obs_asset=min_obs_asset,
    )

    if len(panel) == 0:
        raise RuntimeError("动态 FF6: 面板为空，请检查过滤条件或 z_cols。")

    K = len(factor_cols)
    L = len(z_cols)

    # 提取矩阵
    F = panel[factor_cols].values          # f_{t+1}, shape (N, K)
    Z = panel[z_cols].values               # z_{i,t}, shape (N, L)
    Y = panel["ret_fwd"].values            # r_{i,t+1}, shape (N,)

    # 构造 Kronecker 特征 X_n = f_n ⊗ z_n, shape (N, K*L)
    # einsum: 对每一行做外积
    X = np.einsum("nk,nl->nkl", F, Z).reshape(len(panel), K * L)

    # Pooled OLS (无截距)：theta = vec(Gamma_beta)
    theta, _, _, _ = lstsq(X, Y, rcond=None)

    # 还原 Gamma_beta: K x L
    Gamma_beta = theta.reshape(K, L)

    # 利用 Gamma_beta 计算每个 (i,t) 的 beta_{i,t} 和拟合收益
    # beta_{i,t}: (L,) @ (LxK)^T -> (K,)
    beta_it = Z @ Gamma_beta.T            # shape (N, K)
    y_hat = np.sum(beta_it * F, axis=1)   # element-wise 乘以 f_{t+1} 后求和

    panel_out = panel.copy()
    panel_out["ret_hat_dyn"] = y_hat

    # R^2_total for dynamic FF6
    ss_res = np.sum((Y - y_hat) ** 2)
    ss_tot = np.sum(Y ** 2)
    R2_total_dynamic = 1 - ss_res / ss_tot

    return R2_total_dynamic, Gamma_beta, panel_out
