from data_loader import load_feature_data
from factors_static import build_crypto_ff6
from ff_static_model import run_static_factor_model

PATH = "/Users/elviral/codeproject/capstone/data/features_final.csv"

# 1. 加载数据
df = load_feature_data(PATH)


# 2. 构造 FF6 因子
factors = build_crypto_ff6(df)




# 定义不同规格（FF1 ~ FF6）
specs = {
    "FF1": ['MKT'],
    "FF2": ['MKT', 'SIZE'],
    "FF3": ['MKT', 'SIZE', 'MOM'],
    "FF4": ['MKT', 'SIZE', 'MOM', 'LIQ'],
    "FF5": ['MKT', 'SIZE', 'MOM', 'LIQ', 'VOL'],
    "FF6": ['MKT', 'SIZE', 'MOM', 'LIQ', 'VOL', 'REV'],
}

for name, fcols in specs.items():
    R2_total, betas, _ = run_static_factor_model(
        df=df,
        factors=factors,
        factor_cols=fcols,
        ret_col='asset_excess_return',
        min_obs=0,
    )
    print(f"{name}: R2_total = {R2_total:.4f}, #assets = {len(betas)}")
    # print("factors columns:", factors.columns.tolist())
    # print(factors.head())
