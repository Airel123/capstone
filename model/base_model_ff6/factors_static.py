# factors_static.py
import numpy as np
import pandas as pd

def build_mkt_factor(df: pd.DataFrame,
                     mkt_col: str = 'market_excess_return') -> pd.Series:
    """
    市场因子 MKT: 这里直接用 market_excess_return（假定同日所有币相同）。
    如果未来你有真正的市值加权市场组合，可以在这里替换实现。
    """
    mkt = (
        df.groupby('date')[mkt_col]
          .mean()           # 如果每行都一样，这里等于取唯一值
          .rename('MKT')
    )
    return mkt


def build_long_short_factor(df: pd.DataFrame,
                            char_col: str,
                            long_side: str,
                            q: int = 5,
                            ret_col: str = 'asset_excess_return',
                            weight_col: str = 'size') -> pd.Series:
    """
    通用多空组合因子:
    - 用 (t-1) 的特征 char_col_lag 排序
    - 用 t 的 ret_col 作为收益
    - 每日按分位数分成 q 组
    - long_side='high' => High - Low
      long_side='low'  => Low  - High
    - 默认用 weight_col 做权重；当 weight_col == char_col 时不会重复列
    """

    # 1) 构造所需列，避免重复列名
    base_cols = ['date', 'Symbol', ret_col, char_col]
    if weight_col not in base_cols:
        base_cols.append(weight_col)

    use = df[base_cols].copy()

    # 2) 滞后特征：按币种对 char_col 做 shift(1)
    use[char_col + '_lag'] = use.groupby('Symbol')[char_col].shift(1)

    # 分组排序用 lag，收益要有值
    use = use.dropna(subset=[char_col + '_lag', ret_col])

    # 3) 每日按分位数分桶
    def assign_bucket(x):
        x['bucket'] = pd.qcut(
            x[char_col + '_lag'],
            q,
            labels=False,
            duplicates='drop'
        ) + 1
        return x

    use = use.groupby('date', group_keys=False).apply(assign_bucket)

    # 4) 每日 bucket 收益（价值加权）
    def bucket_ret(x):
        res = {}
        for k in range(1, q + 1):
            sub = x[x['bucket'] == k]
            if len(sub) == 0:
                res[f'Q{k}'] = np.nan
                continue

            # 处理权重
            if weight_col in sub.columns:
                w = sub[weight_col].astype(float).values
                # 如果权重非正或全 0，退回等权
                if np.all(w > 0) and w.sum() > 0:
                    res[f'Q{k}'] = np.average(sub[ret_col].values, weights=w)
                else:
                    res[f'Q{k}'] = sub[ret_col].mean()
            else:
                # 理论上不会进来，保险兜底
                res[f'Q{k}'] = sub[ret_col].mean()

        return pd.Series(res)

    ports = use.groupby('date').apply(bucket_ret)

    if 'Q1' not in ports.columns or 'Q5' not in ports.columns:
        raise ValueError(f"{char_col}: 分组失败，检查该因子是否在大部分日期有足够截面样本。")

    # 5) High-Low or Low-High
    if long_side == 'high':
        factor = ports['Q5'] - ports['Q1']
    else:
        factor = ports['Q1'] - ports['Q5']

    return factor.rename(char_col.upper())


def build_crypto_ff6(df: pd.DataFrame) -> pd.DataFrame:
    """
    构造论文口径的 6 条因子：
    MKT, SIZE, MOM, LIQ, VOL, REV
    """

    # 1) MKT
    MKT = build_mkt_factor(df)  # 名称已是 'MKT'

    # 2) SIZE: Small - Big => long_side='low'
    SIZE = build_long_short_factor(df, char_col='size', long_side='low')
    SIZE = SIZE.rename('SIZE')

    # 3) MOM: Winners - Losers (21d) => long_side='high'
    MOM = build_long_short_factor(df, char_col='mom_21', long_side='high')
    MOM = MOM.rename('MOM')

    # 4) LIQ: Illiquid - Liquid (这里用 bid-ask) => long_side='high'
    LIQ = build_long_short_factor(df, char_col='bid-ask', long_side='high')
    LIQ = LIQ.rename('LIQ')

    # 5) VOL: High Vol - Low Vol => long_side='high'
    VOL = build_long_short_factor(df, char_col='rvol_yz_30', long_side='high')
    VOL = VOL.rename('VOL')

    # 6) REV: Losers - Winners (短期反转) => long_side='low'
    REV = build_long_short_factor(df, char_col='rev', long_side='low')
    REV = REV.rename('REV')

    factors = pd.concat([MKT, SIZE, MOM, LIQ, VOL, REV], axis=1).dropna()
    # factors = pd.concat([MKT, SIZE, MOM, LIQ, VOL, REV], axis=1).ffill()

    return factors
