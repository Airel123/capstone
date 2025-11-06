import pandas as pd
import sys

# --- 1. 配置 ---

# 这是你完成【阶段一】后保存的文件
# (即，进行了重命名、填充NaN、调整单位后的文件)
# !! 如果你保存的文件名不同，请修改下面这个变量 !!
input_file = '/Users/elviral/codeproject/capstone/data/raw_dataset.csv' 

output_file = 'features_group1_only.csv'

# 组 1 特征 (来自阶段一)
group_1_features = [
    'new_add',
    'active_add',
    'transaction_count',
    'avg_trans_value',
    'trading_volume',
    'size'
]

# 我们还需要保留标识符列
identifier_cols = ['Symbol', 'date']

cols_to_keep = identifier_cols + group_1_features

# --- 2. 加载和抽取 ---
print(f"正在加载: {input_file}...")
try:
    # 'usecols' 是一个优化，它只从磁盘读取这些列
    df = pd.read_csv(input_file, usecols=cols_to_keep)
except FileNotFoundError:
    print(f"错误：文件 '{input_file}' 未找到。", file=sys.stderr)
    print(f"请确保你已将【阶段一】的输出保存为此文件名，或修改脚本中的 'input_file' 变量。", file=sys.stderr)
    sys.exit(1)
except ValueError as e:
    # 这个错误通常发生在 'usecols' 里的列名在CSV中不存在时
    print(f"错误: {e}", file=sys.stderr)
    print(f"请检查 {input_file} 中是否包含所有这些列: {cols_to_keep}", file=sys.stderr)
    sys.exit(1)
    
# --- 3. 保存 ---
print(f"正在将 组1 特征抽取并保存到: {output_file}...")
df.to_csv(output_file, index=False)

print("\n--- 组1 特征抽取完成 ---")
print(f"文件已保存: {output_file}")
print("数据预览 (前5行):")
print(df.head())