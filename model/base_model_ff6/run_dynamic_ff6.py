# run_dynamic_ff6.py

from data_loader import load_feature_data
from factors_static import build_crypto_ff6
from dynamic_ff6_model import run_dynamic_ff6

PATH = "/Users/elviral/codeproject/capstone/data/features_final.csv"

# 1. 加载数据
df = load_feature_data(PATH)

# 2. 构造 FF6 因子 (静态多空因子)
factors = build_crypto_ff6(df)

# 3. 选择特征 z_cols （可根据论文 / 你已有因子调整）
# 建议起步版：size / mom / rev / vol / illiq / volume
z_cols = [
    "size",
    "mom_21",
    "rev",
    "rvol_yz_30",
    "illiq",
    "trading_volume",
]

# 4. 运行动态 FF6
R2_dyn, Gamma_beta, panel_dyn = run_dynamic_ff6(
    df=df,
    factors=factors,
    z_cols=z_cols,
    ret_col="asset_excess_return",
    min_obs_asset=20,   # 或 0，根据你意愿
)

print(f"Dynamic FF6: R2_total = {R2_dyn:.4f}")
print("Gamma_beta shape:", Gamma_beta.shape)
print("Gamma_beta (rows=factors, cols=chars):")
print(Gamma_beta)
