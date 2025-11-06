import requests
import pandas as pd
import json
import time
import sys
import os

# --- 1. 配置 ---
API_KEY = "adf27649427a1c3b70e555c13ce8d299b08735be695310a5edc6b9509eaa0ff5"
WHITELIST_FILE = "./output/blockchain_coin_whitelist.json"
API_URL = "https://min-api.cryptocompare.com/data/blockchain/histo/day"
LIMIT = 2000
OUTPUT_FILE = "onchain_data.csv"
START_DATE = "2020-01-01"
END_DATE = "2024-12-30"
start_timestamp = int(pd.Timestamp(START_DATE).timestamp())
end_timestamp = int(pd.Timestamp(END_DATE).timestamp())

# --- 2. 检查设置 ---
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

    while True:
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
                break 

            data = res.json()

            if 'Response' in data and data['Response'] == 'Success' and 'Data' in data and 'Data' in data['Data']:
                batch = data['Data']['Data']
                if not batch:
                    break  # API 没有更多数据了

                earliest_time_in_batch = batch[0]['time']
                
                valid_entries_in_batch = 0
                for entry in batch:
                    if entry['time'] >= start_timestamp:
                        entry["Symbol"] = symbol
                        symbol_data.append(entry)
                        valid_entries_in_batch += 1
                
                print(f"   ...抓取到 {len(batch)} 条记录, {valid_entries_in_batch} 条在时间窗口内。最早日期: {pd.to_datetime(earliest_time_in_batch, unit='s').date()}")

                if earliest_time_in_batch < start_timestamp:
                    break
                
                toTs = earliest_time_in_batch - 86400
                time.sleep(1.1)
            
            else:
                # --- [改进的日志] ---
                # API 返回了 200 OK，但 JSON 结构不符合预期 (例如 {"Response": "Error", ...} 或 {"Response": "Success", "Data": {}})
                
                # 尝试获取 'Message'，如果找不到，就设置一个默认值
                error_message = data.get('Message', 'No "Message" key found in response.')
                
                print(f"   ⚠️ {symbol} 的 API 响应异常: {error_message}")
                print(f"   Full Response (用于调试): {str(data)[:200]}...") # 打印完整的响应内容
                break # 停止此币种的抓取
                # --- [日志改进结束] ---
        
        except Exception as e:
            print(f"   ❌ 抓取 {symbol} 过程中发生意外错误: {e}")
            break 

    all_onchain_data.extend(symbol_data)
    if len(symbol_data) > 0:
        print(f"   ✅ {symbol} 完成, 共获取 {len(symbol_data)} 条有效记录。")
    else:
        print(f"   ℹ️ {symbol} 完成, 未获取到时间窗口内的有效记录。")


# --- 6. 转换为 DataFrame 并处理字段 ---
print("\n--- 抓取完成，正在处理数据 ---")
if not all_onchain_data:
    print("❌ 最终没有成功抓取任何数据！")
    sys.exit(0)

df = pd.DataFrame(all_onchain_data)
df['date'] = pd.to_datetime(df['time'], unit='s')
expected_columns = [
    "Symbol", 
    "date", 
    "time",
    "active_addresses", 
    "average_transaction_value", 
    "new_addresses", 
    "transaction_count"
]
df_final = df.reindex(columns=expected_columns)

# --- 7. 保存 CSV ---
df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
print(f"\n🎉 链上数据抓取完成，已保存为 {OUTPUT_FILE}")
print(f"   总行数: {len(df_final)}")
print(f"   独特币种数: {df_final['Symbol'].nunique()}")
print("\n--- 数据预览 (前5行) ---")
print(df_final.head())