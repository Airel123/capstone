# import requests
# import pandas as pd
# import time

# # 读取已筛选的币种列表（filtered_500_cryptos.csv）
# filtered_cryptos = pd.read_csv("filtered_500_cryptos.csv")

# # API 端点
# OHLCV_URL = "https://min-api.cryptocompare.com/data/v2/histoday"
# CURRENCY = "USD"
# LIMIT = 2000  # 每次 API 请求最大获取天数

# # 设定时间范围
# START_DATE = "2016-12-02"
# END_DATE = "2021-07-09"
# start_timestamp = int(pd.Timestamp(START_DATE).timestamp())  # 起始时间
# end_timestamp = int(pd.Timestamp(END_DATE).timestamp())  # 结束时间

# # 存储所有币种数据
# all_ohlcv_data = []

# def fetch_data(symbol):
#     """调用 API 获取单个币种的 OHLCV 数据"""
#     toTs = end_timestamp  # 从结束时间开始获取数据
#     ohlcv_data = []

#     while toTs > start_timestamp:
#         params = {
#             "fsym": symbol,
#             "tsym": CURRENCY,
#             "limit": LIMIT,
#             "toTs": toTs
#         }
#         response = requests.get(OHLCV_URL, params=params)
#         data = response.json()

#         if 'Data' in data and 'Data' in data['Data']:
#             batch_data = data['Data']['Data']
#             if not batch_data:
#                 break  # 没有数据就停止
#             for entry in batch_data:
#                 entry["Symbol"] = symbol  # 增加币种信息
#             ohlcv_data.extend(batch_data)
#             toTs = batch_data[0]['time'] - 86400  # 继续请求更早的数据
#         else:
#             break  # API 返回异常，停止请求
        
#         time.sleep(1)  # 避免 API 速率限制

#     return ohlcv_data

# # 遍历所有币种
# for symbol in filtered_cryptos["Symbol"]:
#     print(f"📊 正在获取 {symbol} 的 OHLCV 数据...")
#     data = fetch_data(symbol)
#     all_ohlcv_data.extend(data)

# # 转换为 Pandas DataFrame
# df_ohlcv = pd.DataFrame(all_ohlcv_data)

# # 处理时间戳
# df_ohlcv['date'] = pd.to_datetime(df_ohlcv['time'], unit='s')

# # 重新排序列
# df_ohlcv = df_ohlcv[["Symbol", "date", "open", "high", "low", "close", "volumeto"]]

# # 保存合并后的 CSV 文件
# csv_filename = "all_cryptos_ohlcv.csv"
# df_ohlcv.to_csv(csv_filename, index=False, encoding="utf-8")

# print(f"✅ 全部币种 OHLCV 数据获取完成，已保存为 {csv_filename}！")


# # === 读取币种列表 ===
# crypto_df = pd.read_csv("data preparation/data combination/data source/filtered_500_cryptos.csv")
# symbols = crypto_df["Symbol"].dropna().unique().tolist()

# # === API 设置 ===
# OHLCV_URL = "https://min-api.cryptocompare.com/data/v2/histoday"
# CURRENCY = "USD"
# LIMIT = 2000  # 最大支持 2000 条

# # === 时间范围设置 ===
# START_DATE = "2020-01-01"
# END_DATE = "2024-12-30"
# start_timestamp = int(pd.Timestamp(START_DATE).timestamp())
# end_timestamp = int(pd.Timestamp(END_DATE).timestamp())

# # === 存储所有币种的数据 ===
# all_ohlcv_data = []

# # === 单币种数据抓取函数 ===
# def fetch_ohlcv(symbol):
#     all_data = []
#     toTs = end_timestamp

#     while toTs > start_timestamp:
#         params = {
#             "fsym": symbol,
#             "tsym": CURRENCY,
#             "limit": LIMIT,
#             "toTs": toTs
#         }

#         try:
#             res = requests.get(OHLCV_URL, params=params)
#             data = res.json()

#             if 'Data' in data and 'Data' in data['Data']:
#                 batch = data['Data']['Data']
#                 if not batch:
#                     break

#                 for entry in batch:
#                     entry["Symbol"] = symbol
#                     # volumefrom 保留
#                     entry["volumefrom"] = entry.get("volumefrom", None)
#                     entry["volumeto"] = entry.get("volumeto", None)

#                 all_data.extend(batch)
#                 toTs = batch[0]['time'] - 86400
#                 time.sleep(1)  # 避免 API 限速
#             else:
#                 break
#         except Exception as e:
#             print(f"⚠️ 抓取 {symbol} 失败: {e}")
#             break

#     return all_data

# # === 遍历抓取全部币种 ===
# for symbol in symbols:
#     print(f"📊 正在获取 {symbol}/USD 的 OHLCV 数据...")
#     data = fetch_ohlcv(symbol)

#     if not data:
#         print(f"⚠️ 无法获取 {symbol}，已跳过。")
#         continue

#     all_ohlcv_data.extend(data)

# # === 转换为 DataFrame 并处理字段 ===
# df = pd.DataFrame(all_ohlcv_data)

# # 如果为空就跳过处理
# if not df.empty:
#     df['date'] = pd.to_datetime(df['time'], unit='s')
#     df = df[["Symbol", "date", "open", "high", "low", "close", "volumefrom", "volumeto"]]

#     # 保存 CSV
#     output_file = "all_359cryptos_ohlcv.csv"
#     df.to_csv(output_file, index=False)
#     print(f"✅ 数据抓取完成，已保存为 {output_file}")
# else:
#     print("❌ 没有成功抓取任何数据！")



import requests
import pandas as pd
import time
crypto_df = pd.read_csv("data preparation/data combination/data source/coin_list.csv")
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
    output_file = "ohlcv_data"
    df.to_csv(output_file, index=False)
    print(f"✅ 数据抓取完成，已保存为 {output_file}")
else:
    print("❌ 没有成功抓取任何数据！")
