import requests
import pandas as pd

# API 端点
API_URL = "https://min-api.cryptocompare.com/data/top/mktcapfull"

# 参数配置
params = {
    "limit": 100,  # API 最大支持 100，一次获取 100
    "tsym": "USD",
    "assetClass": "ALL"
}

# 已知稳定币列表
STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "PAX", "GUSD", "LUSD", "sUSD", "HUSD", "FEI", "FRAX", "USTC"}

# 存储所有数据
all_data = []

# 获取前 500 名币种数据（分批获取，每次 100）
for page in range(5):  # 5 * 100 = 500
    params["page"] = page  # 设置分页参数
    response = requests.get(API_URL, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if "Data" in data:
            all_data.extend(data["Data"])
    else:
        print(f"请求失败，状态码: {response.status_code}")

# 提取关键信息
coins_list = []
for coin in all_data:
    coin_info = coin.get("CoinInfo", {})
    raw_info = coin.get("RAW", {}).get("USD", {})

    symbol = coin_info.get("Name", "")
    market_cap = raw_info.get("MKTCAP", 0)

    # 过滤稳定币 & 市值低于 1 亿美元的币
    # if symbol not in STABLECOINS and market_cap >= 100_000_000:
    if True:
        coins_list.append({
            "Rank": coin_info.get("SortOrder"),
            "Symbol": symbol,
            "FullName": coin_info.get("FullName"),
            "MarketCap": market_cap,
            "Price": raw_info.get("PRICE", "N/A"),
            "Volume24h": raw_info.get("VOLUME24HOUR", "N/A"),
            "Change24h": raw_info.get("CHANGEPCT24HOUR", "N/A")
        })

# 转换为 Pandas DataFrame
df = pd.DataFrame(coins_list)

# 处理 MarketCap 数据类型问题
df["MarketCap"] = pd.to_numeric(df["MarketCap"], errors="coerce")  # 转换为 float，无法转换的值设为 NaN

# 去除 NaN 值，并按 MarketCap 排序
df = df.dropna(subset=["MarketCap"])  # 删除 MarketCap 为空的行
df = df.sort_values(by="MarketCap", ascending=False)

# 保存 CSV
csv_filename = "api_500_cryptos.csv"
df.to_csv(csv_filename, index=False, encoding="utf-8")

print(f"✅ 筛选完成，已生成 CSV 文件: {csv_filename}")
