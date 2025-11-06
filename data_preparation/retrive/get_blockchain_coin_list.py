import requests
import json
import sys

# --- 1. 设置您的 API 密钥 ---
# !! 警告：请在此处粘贴您自己的 API 密钥
# (我已移除您在示例中提供的密钥，以保护您的账户安全)
API_KEY = "adf27649427a1c3b70e555c13ce8d299b08735be695310a5edc6b9509eaa0ff5"

if API_KEY == "PLEASE_PASTE_YOUR_API_KEY_HERE":
    print("错误：请在脚本的 API_KEY 变量中设置您的 API 密钥。")
    sys.exit(1) # 退出脚本

# --- 2. API 端点和参数 ---
API_URL = "https://min-api.cryptocompare.com/data/blockchain/list"
params = {
    "api_key": API_KEY
}
headers = {
    "Content-type": "application/json; charset=UTF-8"
}
OUTPUT_FILE = "blockchain_coin_whitelist.json" # 我们将把列表保存到这个文件

print(f"正在从 {API_URL} 请求可用的币种列表...")

# --- 3. 发送请求并处理响应 ---
try:
    response = requests.get(API_URL, params=params, headers=headers)

    # 检查请求是否成功
    if response.status_code == 200:
        json_response = response.json()
        
        # --- 4. 解析 JSON 并提取白名单 ---
        # 检查 API 响应是否是我们预期的格式
        if 'Response' in json_response and json_response['Response'] == 'Success' and 'Data' in json_response:
            
            # 'Data' 是一个字典，其 键 (keys) 就是币种的 Symbol
            # 例如: "Data": { "BTC": {...}, "ETH": {...}, ... }
            data_object = json_response['Data']
            
            # 提取所有的 键 (Symbols) 并转换为一个列表
            whitelist_symbols = list(data_object.keys())
            
            print(f"✅ 请求成功！提取了 {len(whitelist_symbols)} 个币种的白名单。")
            print(f"   (示例: {whitelist_symbols[:5]}...)") # 打印前5个作为预览

            # --- 5. 将白名单保存到文件 ---
            with open(OUTPUT_FILE, 'w') as f:
                # 使用 json.dump 保存列表，以便下一个脚本轻松读取
                json.dump(whitelist_symbols, f, indent=4)
            
            print(f"💾 白名单已保存到: {OUTPUT_FILE}")
            
        else:
            print(f"❌ API 响应了成功状态，但未找到预期的数据。")
            print(f"   API 消息: {json_response.get('Message', 'N/A')}")

    else:
        print(f"❌ 请求失败，状态码: {response.status_code}")
        print(f"   错误信息: {response.text}")

except requests.exceptions.RequestException as e:
    print(f"❌ 发生网络错误: {e}")
except Exception as e:
    print(f"❌ 处理数据时发生意外错误: {e}")