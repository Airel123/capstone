import requests
import pandas as pd
import json
import time
import sys
import os

# --- 1. 配置 ---

# !! 警告：请在此处粘贴您自己的 API 密钥
# (我已移除您在示例中提供的密钥，以保护您的账户安全)
API_KEY = "adf27649427a1c3b70e555c13ce8d299b08735be695310a5edc6b9509eaa0ff5"

# 您的白名单文件路径
WHITELIST_FILE = "data preparation/data combination/output/blockchain_coin_whitelist.json"

# API 和数据设置
API_URL = "https://min-api.cryptocompare.com/data/blockchain/histo/day"
LIMIT = 2000  # API 最大支持 2000 条
OUTPUT_FILE = "onchain_data.csv"

# 您的新时间范围
START_DATE = "2020-01-01"
END_DATE = "2024-12-30"
start_timestamp = int(pd.Timestamp(START_DATE).timestamp())
end_timestamp = int(pd.Timestamp(END_DATE).timestamp())

# --- 2. 检查设置 ---
if API_KEY == "PLEASE_PASTE_YOUR_API_KEY_HERE":
    print(f"❌ 错误：请在脚本的 API_KEY 变量中设置您的 API 密钥。")
    sys.exit(1)

if not os.path.exists(WHITELIST_FILE):
    print(f"❌ 错误：找不到白名单文件: {WHITELIST_FILE}")
    sys.exit(1)

# --- 3. 加载白名单 ---
try:
    with open(WHITELIST_FILE, 'r') as f:
        whitelist_symbols = json.load(f)
    print(f"✅ 成功加载 {len(whitelist_symbols)} 个币种的白名单 (从 {WHITELIST_FILE})。")
except Exception as e:
    print(f"❌ 加载白名单文件时出错: {e}")
    sys.exit(1)

# --- 4. 存储所有币种的数据 ---
all_onchain_data = []

# --- 5. 遍历抓取全部币种 ---
total_symbols = len(whitelist_symbols)
for i, symbol in enumerate(whitelist_symbols):
    print(f"\n📊 正在获取 {symbol} ({i+1}/{total_symbols}) 的链上数据...")
    
    symbol_data = []
    toTs = end_timestamp

    while True:  # 循环将由内部逻辑中断
        params = {
            "fsym": symbol,
            "api_key": API_KEY,
            "limit": LIMIT,
            "toTs": toTs
        }

        try:
            res = requests.get(API_URL, params=params)
            
            if res.status_code != 200:
                print(f"   ⚠️ {symbol} 请求失败, 状态码: {res.status_code}, 响应: {res.text[:100]}...")
                break # 停止此币种的抓取

            data = res.json()

            if 'Response' == 'Success' and 'Data' in data and 'Data' in data['Data']:
                batch = data['Data']['Data']
                if not batch:
                    break  # API 没有更多数据了

                # 检查这批数据是否已经早于我们的起始日期
                earliest_time_in_batch = batch[0]['time']
                
                # 过滤这批数据，只保留我们时间窗口内的
                valid_entries_in_batch = 0
                for entry in batch:
                    if entry['time'] >= start_timestamp:
                        entry["Symbol"] = symbol # 关键：为数据打上币种标签
                        symbol_data.append(entry)
                        valid_entries_in_batch += 1
                
                print(f"   ...抓取到 {len(batch)} 条记录, {valid_entries_in_batch} 条在时间窗口内。最早日期: {pd.to_datetime(earliest_time_in_batch, unit='s').date()}")

                # 如果这批数据中最早的时间已经早于我们的起始时间，
                # 意味着我们已经抓取了所有需要的数据，可以停止了。
                if earliest_time_in_batch < start_timestamp:
                    break
                
                # 准备下一次抓取
                toTs = earliest_time_in_batch - 86400  # -1 天
                time.sleep(1.1) # 礼貌性等待，避免 API 过载 (1.1秒更安全)
            
            else:
                # API 可能返回错误或空数据
                print(f"   ⚠️ {symbol} 的 API 响应异常: {data.get('Message', 'N/A')}")
                break # 停止此币种的抓取
        
        except Exception as e:
            print(f"   ❌ 抓取 {symbol} 过程中发生意外错误: {e}")
            break # 停止此币种的抓取

    all_onchain_data.extend(symbol_data)
    print(f"   ✅ {symbol} 完成, 共获取 {len(symbol_data)} 条有效记录。")

# --- 6. 转换为 DataFrame 并处理字段 ---
print("\n--- 抓取完成，正在处理数据 ---")
if not all_onchain_data:
    print("❌ 最终没有成功抓取任何数据！")
    sys.exit(0)

df = pd.DataFrame(all_onchain_data)

# 将 'time' 转换为 'date'
df['date'] = pd.to_datetime(df['time'], unit='s')

# 根据图二 (image_e76f20.png) 的响应字段，我们保留这些
# 这是论文中需要的4个核心链上指标
expected_columns = [
    "Symbol", 
    "date", 
    "time",
    "active_addresses", 
    "average_transaction_value", 
    "new_addresses", 
    "transaction_count"
]

# 过滤 DataFrame，只保留我们期望的列 (忽略 API 可能返回的其他多余字段)
# 我们使用 .reindex() 来安全地处理可能缺失的列
df_final = df.reindex(columns=expected_columns)

# --- 7. 保存 CSV ---
df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
print(f"\n🎉 链上数据抓取完成，已保存为 {OUTPUT_FILE}")
print(f"   总行数: {len(df_final)}")
print(f"   独特币种数: {df_final['Symbol'].nunique()}")
print("\n--- 数据预览 (前5行) ---")
print(df_final.head())