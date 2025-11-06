import requests
import pandas as pd
import time
crypto_df = pd.read_csv("C:/Users/Air/Documents/local_code/capstoneProject/capstone/data/cryptocompare_all_coins.csv")
symbols = crypto_df["Symbol"].dropna().unique().tolist()

# === API 设置 ===
OHLCV_URL = "https://min-api.cryptocompare.com/data/v2/histoday"
CURRENCY = "USD"
LIMIT = 2000  # 最大支持 2000 条

# === 时间范围设置 ===
START_DATE = "2020-01-01"
END_DATE = "2024-12-30"
start_timestamp = int(pd.Timestamp(START_DATE).timestamp())
end_timestamp = int(pd.Timestamp(END_DATE).timestamp())

# === 存储所有币种的数据 ===
all_ohlcv_data = []
# === 单币种数据抓取函数 (已修复) ===
def fetch_ohlcv(symbol):
    all_data = []
    toTs = end_timestamp

    while True:  # 循环将由内部逻辑中断
        params = {
            "fsym": symbol,
            "tsym": CURRENCY,
            "limit": LIMIT,
            "toTs": toTs
        }

        try:
            res = requests.get(OHLCV_URL, params=params)
            data = res.json()

            if 'Data' in data and 'Data' in data['Data']:
                batch = data['Data']['Data']
                if not batch:
                    break  # API 没有更多数据了

                # 关键：检查这批数据是否已经早于我们的起始日期
                earliest_time_in_batch = batch[0]['time']
                
                # 过滤这批数据，只保留我们时间窗口内的
                valid_entries = []
                for entry in batch:
                    if entry['time'] >= start_timestamp:
                        entry["Symbol"] = symbol
                        entry["volumefrom"] = entry.get("volumefrom", None)
                        entry["volumeto"] = entry.get("volumeto", None)
                        valid_entries.append(entry)
                
                all_data.extend(valid_entries)

                # 如果这批数据中最早的时间已经早于我们的起始时间，
                # 意味着我们已经抓取了所有需要的数据，可以停止了。
                if earliest_time_in_batch < start_timestamp:
                    break
                
                # 准备下一次抓取
                toTs = earliest_time_in_batch - 86400  # -1 天
                time.sleep(1)
            else:
                # API 可能返回错误或空数据
                print(f"⚠️ {symbol} 的 API 响应异常: {data.get('Message', 'N/A')}")
                break
        except Exception as e:
            print(f"⚠️ 抓取 {symbol} 失败: {e}")
            break

    return all_data



# === 遍历抓取全部币种 ===
for symbol in symbols:
    print(f"📊 正在获取 {symbol}/USD 的 OHLCV 数据...")
    data = fetch_ohlcv(symbol)

    if not data:
        print(f"⚠️ 无法获取 {symbol}，已跳过。")
        continue

    all_ohlcv_data.extend(data)

# === 转换为 DataFrame 并处理字段 ===
df = pd.DataFrame(all_ohlcv_data)

# 如果为空就跳过处理
if not df.empty:
    df['date'] = pd.to_datetime(df['time'], unit='s')
    df = df[["Symbol", "date", "open", "high", "low", "close", "volumefrom", "volumeto"]]

    # 保存 CSV
    output_file = "cryptocompare_all_coins_ohlcv_dailycode"
    df.to_csv(output_file, index=False)
    print(f"✅ 数据抓取完成，已保存为 {output_file}")
else:
    print("❌ 没有成功抓取任何数据！")
