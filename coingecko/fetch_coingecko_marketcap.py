import requests
import pandas as pd
import time
import os
import sys
import json

# --- 1. 配置 ---

# !! 关键修复 !!
# 请在此处粘贴您从 CoinGecko 获取的免费 "Demo API" 密钥
CG_API_KEY = "CG-ztwMUqbjq2bxkVQZiRAsdFkk"

COIN_LIST_FILE = "data_preparation\data_combination\data_source\coin_list.csv"  # 您上传的币种列表文件
OUTPUT_FILE = "marketcap_data.csv"
START_DATE = "2024-12-01"
END_DATE = "2025-10-01"

# CoinGecko API URL
CG_BASE_URL = "https://api.coingecko.com/api/v3"

# 将日期转换为 CoinGecko 需要的 Unix 时间戳
start_timestamp = int(pd.Timestamp(START_DATE).timestamp())
end_timestamp = int(pd.Timestamp(END_DATE).timestamp())


# --- 检查 API 密钥是否已设置 ---
if CG_API_KEY == "PASTE_YOUR_COINGECKO_DEMO_KEY_HERE" or not CG_API_KEY:
    print("❌ 错误: 请在脚本的 'CG_API_KEY' 变量中设置您的 CoinGecko API 密钥。")
    print("   这对于修复 401 错误至关重要。")
    sys.exit(1)


# --- 2. 步骤 1: 获取 CoinGecko 完整币种列表 (创建映射) ---
def get_coingecko_map():
    """
    调用 /coins/list API, 保存完整列表, 并创建一个 {symbol_lowercase: id} 映射.
    (此函数已在 v3 中修复，此处保留)
    """
    print("正在从 CoinGecko 获取完整币种列表 ( /coins/list )...")
    url = f"{CG_BASE_URL}/coins/list"
    try:
        res = requests.get(url)
        if res.status_code != 200:
            print(f"错误: 无法获取 /coins/list. 状态码: {res.status_code}")
            return None, None
        
        coins_list_full_raw = res.json()
        output_list_file = "coingecko_full_list.json"
        with open(output_list_file, 'w', encoding='utf-8') as f:
            json.dump(coins_list_full_raw, f, indent=4)
        print(f"✅ 成功获取 {len(coins_list_full_raw)} 个币种。")
        print(f"   已将完整列表保存到: {output_list_file}")

        # 映射逻辑 (v3 修复):
        id_map = {}
        for coin in coins_list_full_raw:
            symbol = coin['symbol'].lower()
            if symbol not in id_map:  # 只添加第一个匹配项
                id_map[symbol] = coin['id']
                
        return id_map, coins_list_full_raw
        
    except Exception as e:
        print(f"❌ 获取 CoinGecko 列表时出错: {e}")
        return None, None

# --- 3. 步骤 2: 加载您的目标币种列表 ---
def load_target_symbols(filename):
    if not os.path.exists(filename):
        print(f"❌ 错误: 找不到文件 '{filename}'")
        return None
    df = pd.read_csv(filename)
    if 'Symbol' not in df.columns:
        print(f"❌ 错误: '{filename}' 中未找到 'Symbol' 列。")
        return None
    return df['Symbol'].dropna().unique().tolist()

# --- 4. 步骤 3: 循环下载数据 ---
def fetch_market_caps(target_symbols, cg_map, cg_full_list):
    all_data = []
    
    # --- 创建并保存详细的映射文件 ---
    print("--- 正在创建映射报告 ---")
    mapping_data = []
    for symbol in target_symbols:
        symbol_lower = symbol.lower()
        cg_id = cg_map.get(symbol_lower) # 使用我们修复后的 id_map
        
        full_name = ""
        if cg_id:
            coin_data = next((item for item in cg_full_list if item["id"] == cg_id), None)
            if coin_data:
                full_name = coin_data.get('name', '')
                
        mapping_data.append({
            "Target_Symbol": symbol,
            "CoinGecko_ID": cg_id,
            "CoinGecko_Name": full_name,
            "Status": "Matched" if cg_id else "Not_Matched"
        })

    mapping_df = pd.DataFrame(mapping_data)
    mapping_df.to_csv("coingecko_mapping.csv", index=False, encoding="utf-8")
    
    matched_map = {item['Target_Symbol']: item['CoinGecko_ID'] for item in mapping_data if item['Status'] == 'Matched'}
    
    print(f"在 CoinGecko 中成功匹配 {len(matched_map)} / {len(target_symbols)} 个币种。")
    print(f"✅ 已将详细的映射报告保存到: coingecko_mapping.csv")
    print("-------------------")
    # ------------------------------------
    
    # 循环下载
    for i, (symbol, cg_id) in enumerate(matched_map.items()):
        print(f"\n📊 ({i+1}/{len(matched_map)}) 正在获取 {symbol} (ID: {cg_id}) 的数据...")
        
        url = f"{CG_BASE_URL}/coins/{cg_id}/market_chart/range"
        
        # --- 关键修复: 使用 'x_cg_demo_api_key' ---
        params = {
            "vs_currency": "usd",
            "from": start_timestamp,
            "to": end_timestamp,
            "x_cg_demo_api_key": CG_API_KEY # 适用于免费 Demo 密钥
        }
        # ------------------------------------
            
        try:
            res = requests.get(url, params=params)
            
            if res.status_code == 200:
                data = res.json()
                market_caps = data.get('market_caps', [])
                
                if not market_caps:
                    print(f"   ⚠️ {symbol} 没有返回市值数据。")
                    continue

                temp_df = pd.DataFrame(market_caps, columns=['time', 'market_cap'])
                temp_df['Symbol'] = symbol
                all_data.append(temp_df)
                print(f"   ✅ {symbol} 完成, 获取 {len(temp_df)} 条记录。")
            
            else:
                print(f"   ❌ {symbol} 请求失败. GECKO 状态码: {res.status_code}")
                try:
                    error_msg = res.json()
                    print(f"   响应: {error_msg}")
                    if res.status_code == 401:
                        print("   !! 401 错误: 您的 API 密钥可能无效、已过期或不正确。")
                    if res.status_code == 429:
                        print("   !! 429 错误: 速率限制被触发。")
                except:
                    print(f"   响应: {res.text[:200]}...")

            # --- 速率限制修复 ---
            # 增加等待时间以匹配免费 API (约 10 次/分钟)
            sleep_time = 6.0 
            print(f"   (暂停 {sleep_time} 秒以避免速率限制)")
            time.sleep(sleep_time)
            # ---------------------

        except Exception as e:
            print(f"   ❌ {symbol} 抓取时发生意外错误: {e}")
            
    return all_data

# --- 5. 主执行逻辑 ---
def main():
    # 步骤 1 & 2
    cg_id_map, cg_full_list = get_coingecko_map()
    target_symbols_list = load_target_symbols(COIN_LIST_FILE)
    
    if not cg_id_map or not target_symbols_list:
        print("❌ 无法完成必要的前置步骤。脚本将退出。")
        return

    # 步骤 3
    all_dfs = fetch_market_caps(target_symbols_list, cg_id_map, cg_full_list)

    # 步骤 4: 合并与保存
    if not all_dfs:
        print("\n❌ 未能获取任何市值数据。")
        return

    print("\n--- 抓取完成，正在合并数据 ---")
    final_df = pd.concat(all_dfs)
    final_df['date'] = pd.to_datetime(final_df['time'], unit='ms').dt.date
    final_df = final_df[['Symbol', 'date', 'market_cap']].drop_duplicates()
    
    final_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"\n🎉 每日市值数据抓取完成!")
    print(f"   已保存为: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()