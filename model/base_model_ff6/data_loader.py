# data_loader.py
import pandas as pd

def load_feature_data(path: str) -> pd.DataFrame:
    """
    加载特征数据，并做基础整理。
    约定输入包含以下列：
    ['Symbol', 'date',
     'asset_excess_return', 'market_excess_return',
     'size', 'mom_21', 'rev', 'rvol_yz_30', 'bid-ask']
    """
    df = pd.read_csv(path)

    # 确保基本列存在（如果缺就报错，方便排查）
    required = [
        'Symbol', 'date',
        'asset_excess_return', 'market_excess_return',
        'size', 'mom_21', 'rev', 'rvol_yz_30', 'bid-ask'
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"缺少必要列: {missing}")

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['date', 'Symbol'])

    # 这里只做轻微清洗，重度过滤你前面脚本已经做过
    # df = df.dropna(subset=['asset_excess_return', 'market_excess_return'])

    # symbol_counts = (
    # df['Symbol']
    # .value_counts()
    # .rename_axis('Symbol')
    # .reset_index(name='count')
    # )
    # print(symbol_counts)



    return df


# if __name__=="__main__":
#     PATH = "/Users/elviral/codeproject/capstone/data/features_final.csv"

#     # 1. 加载数据
#     df = load_feature_data(PATH)