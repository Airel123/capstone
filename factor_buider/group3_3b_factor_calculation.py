import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS

ROLLING_WINDOW_CAPM = 30
MIN_OBS_CAPM = 20

input_file = 'features_03_group3_a.csv'
output_file = 'features_03_group3_b.csv'

print(f"正在加载: {input_file}...")
df = pd.read_csv(input_file)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['Symbol', 'date']).reset_index(drop=True)

# 确保用于回归的列是数值
for col in ['asset_excess_return', 'market_excess_return']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

print("正在计算 CAPM 特征 (alpha, beta, idio_vol) ...")

def apply_rolling_ols(group, window, min_obs, y_col, x_col):
    """
    对单个 Symbol 应用滚动 OLS：y = alpha + beta * x
    返回 index 与 group 对齐的 DataFrame，包含 capm_alpha_30 / capm_beta_30 / idio_vol_30
    """
    # 1) 组装回归数据
    y = group[y_col]
    X = sm.add_constant(group[x_col], has_constant='add')  # 添加常数项
    data = pd.concat([y, X], axis=1).dropna()

    if len(data) < min_obs:
        # 返回空框架但索引对齐
        return pd.DataFrame({
            'capm_alpha_30': pd.Series(index=group.index, dtype='float64'),
            'capm_beta_30':  pd.Series(index=group.index, dtype='float64'),
            'idio_vol_30':   pd.Series(index=group.index, dtype='float64'),
        })

    # 2) Rolling OLS
    # 注意：exog 用 data[['const', x_col]]，保证列名一致
    model = RollingOLS(endog=data[y_col], exog=data[['const', x_col]],
                       window=window, min_nobs=min_obs)
    res = model.fit()

    # 3) 取参数与残差标准差（同窗 RMSE）
    params = res.params.copy()
    params = params.rename(columns={'const': 'capm_alpha_30', x_col: 'capm_beta_30'})
    params['idio_vol_30'] = np.sqrt(res.mse_resid)

    # 4) 对齐到 group 的索引（前 window-1 行自然是 NaN）
    out = params.reindex(group.index)
    return out

# 关键：group_keys=False，避免产生 MultiIndex，便于直接 join
capm_features = df.groupby('Symbol', group_keys=False).apply(
    apply_rolling_ols,
    window=ROLLING_WINDOW_CAPM,
    min_obs=MIN_OBS_CAPM,
    y_col='asset_excess_return',
    x_col='market_excess_return'
)

# 合并回主表
df = df.join(capm_features)

# 保存
df.to_csv(output_file, index=False)
print("✅ CAPM 完成 ->", output_file)

# 预览
print(df[['Symbol','date','capm_alpha_30','capm_beta_30','idio_vol_30']].dropna().head())
