# ff_static_model.py
import pandas as pd
import numpy as np
from numpy.linalg import lstsq

def run_static_factor_model(df: pd.DataFrame,
                            factors: pd.DataFrame,
                            factor_cols,
                            ret_col: str = 'asset_excess_return',
                            min_obs: int = 0):
    """
    静态因子模型（零截距）:
        r_{i,t} = beta_i' f_t + eps_{i,t}
    - df: 包含 [Symbol, date, ret_col] 的面板数据
    - factors: index=date, columns=factor_cols 的因子时间序列
    - factor_cols: 使用的因子名列表（如 ['MKT'], ['MKT','SIZE',...,'REV']）
    - 返回:
        R2_total: 全局拟合优度
        betas: {Symbol: beta_i ndarray}
        fitted: DataFrame[Symbol, date, ret, ret_hat]
    """
    # 合并
    panel = (
        df[['Symbol', 'date', ret_col]]
        .merge(factors[factor_cols], on='date', how='inner')
        .dropna(subset=[ret_col] + list(factor_cols))
    )
    # panel = df.merge(factors, on='date', how='left')


    betas = {}
    fitted_list = []

    for sym, g in panel.groupby('Symbol'):
        if len(g) < min_obs:
            continue

        Y = g[ret_col].values
        X = g[factor_cols].values

        # 零截距 OLS
        b, _, _, _ = lstsq(X, Y, rcond=None)
        betas[sym] = b

        y_hat = X @ b

        fitted_list.append(pd.DataFrame({
            'Symbol': sym,
            'date': g['date'].values,
            'ret': Y,
            'ret_hat': y_hat
        }))

    if not fitted_list:
        raise RuntimeError("没有满足样本长度要求的 Symbol，请检查 min_obs 或数据覆盖。")

    fitted = pd.concat(fitted_list, ignore_index=True)

    ss_res = ((fitted['ret'] - fitted['ret_hat']) ** 2).sum()
    ss_tot = (fitted['ret'] ** 2).sum()
    R2_total = 1 - ss_res / ss_tot

    return R2_total, betas, fitted
