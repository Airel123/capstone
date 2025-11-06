import os
import csv
import time
import requests
import pandas as pd
from typing import Dict, Any, List, Optional

# ======== 可配置区 ========
INPUT_SYMBOLS_CSV = r"C:/Users/Air/Documents/local_code/capstoneProject/capstone/data/cryptocompare_all_coins_symbols.csv"
OUTPUT_CSV = "cryptocompare_ohlcv.csv"   # 统一写这里
OHLCV_URL = "https://min-api.cryptocompare.com/data/v2/histoday"
CURRENCY = "USD"
LIMIT = 2000                 # 每次批量最大条数（CryptoCompare支持）
START_DATE = "2020-01-01"    # 时间范围（含）
END_DATE   = "2024-12-30"
REQUEST_TIMEOUT = 15         # 每次请求超时（秒）
SLEEP_BETWEEN_CALLS = 1.0    # 请求间隔，避免限频
MAX_RETRIES = 3              # 单次HTTP重试
BACKOFF_BASE = 1.5           # 指数退避倍数
# =========================

# 计算时间戳边界
start_ts = int(pd.Timestamp(START_DATE).timestamp())
end_ts = int(pd.Timestamp(END_DATE).timestamp())

# 输出列顺序（严格按这个顺序写）
CSV_COLUMNS = ["Symbol", "date", "open", "high", "low", "close", "volumefrom", "volumeto"]

def read_symbols(csv_path: str) -> List[str]:
    df = pd.read_csv(csv_path)
    symbols = df["Symbol"].dropna().astype(str).str.strip().unique().tolist()
    return symbols

def ensure_csv_with_header(path: str, headers: List[str]) -> None:
    need_header = not os.path.exists(path) or os.path.getsize(path) == 0
    if need_header:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

def safe_get(session: requests.Session, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """带重试与超时的GET；失败返回None"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                # 常见限流或服务错误，指数退避
                time.sleep((BACKOFF_BASE ** (attempt - 1)))
                continue
            return resp.json()
        except requests.RequestException:
            time.sleep((BACKOFF_BASE ** (attempt - 1)))
    return None

def fetch_ohlcv_batches(session: requests.Session, symbol: str):
    """
    迭代抓取单币种从 end_ts 向前的批次数据。
    每次 yield 一个批次(list)，内部不做写入。
    """
    to_ts = end_ts
    while True:
        params = {
            "fsym": symbol,
            "tsym": CURRENCY,
            # "api_key":"adf27649427a1c3b70e555c13ce8d299b08735be695310a5edc6b9509eaa0ff5",
            "limit": LIMIT,
            "toTs": to_ts
        }
        data = safe_get(session, OHLCV_URL, params)
        if not data or "Data" not in data or "Data" not in data["Data"]:
            # API 异常或结构不符合预期：停止该币种
            break

        batch = data["Data"]["Data"]
        if not batch:  # 没有更多数据
            break

        yield batch  # 把原始批次丢出去

        # 下一次请求往前推进一天（按返回批次最早时间点）
        earliest = batch[0].get("time")
        if earliest is None:
            break
        if earliest < start_ts:
            # 下次再请求就会越界（或已经拿够），可以直接结束
            break
        to_ts = earliest - 86400  # 再往前1天
        time.sleep(SLEEP_BETWEEN_CALLS)

def row_from_entry(symbol: str, entry: Dict[str, Any]) -> Optional[List[Any]]:
    """
    把API条目转为一行CSV；做字段兜底与时间窗过滤。
    不在时间窗口内返回 None。
    """
    t = entry.get("time")
    if t is None or t < start_ts or t > end_ts:
        return None

    # 时间转换
    date_str = pd.to_datetime(t, unit="s").strftime("%Y-%m-%d")

    # 字段兜底：有些历史天数据会缺 open/high/low/close，安全起见用 None
    o = entry.get("open")
    h = entry.get("high")
    l = entry.get("low")
    c = entry.get("close")
    vf = entry.get("volumefrom")  # CryptoCompare命名
    vt = entry.get("volumeto")

    return [symbol, date_str, o, h, l, c, vf, vt]

def append_rows(path: str, rows: List[List[Any]]) -> None:
    """把若干行追加进CSV（一批一写；也可改成逐条实时写入）"""
    if not rows:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def main():
    symbols = read_symbols(INPUT_SYMBOLS_CSV)
    print(f"✅ 待抓取币种数：{len(symbols)}")
    ensure_csv_with_header(OUTPUT_CSV, CSV_COLUMNS)

    session = requests.Session()

    for idx, symbol in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] ⛏️ 抓取 {symbol}/USD ...")
        written_for_symbol = 0
        try:
            for batch in fetch_ohlcv_batches(session, symbol):
                # 将该批次转换并立即写入（逐条写）
                rows_to_write = []
                for entry in batch:
                    try:
                        row = row_from_entry(symbol, entry)
                        if row is None:
                            continue
                        # 逐条写入（也可以改为每N条写一次以减少IO）
                        append_rows(OUTPUT_CSV, [row])
                        written_for_symbol += 1
                    except Exception as e_item:
                        # 单条坏数据直接跳
                        print(f"  ↳ ⚠️ {symbol} 某条记录解析失败，已跳过：{e_item}")
                # 轻微停顿以避免过快
                time.sleep(0.2)
        except Exception as e_symbol:
            print(f"⚠️ 抓取 {symbol} 时发生异常，已跳过该币种：{e_symbol}")
            continue

        if written_for_symbol == 0:
            print(f"⚠️ {symbol} 在指定区间内无可写数据（或全部失败）。")
        else:
            print(f"✅ {symbol} 完成，已写入 {written_for_symbol} 行。")

    print("🎉 全部完成。数据写入：", os.path.abspath(OUTPUT_CSV))

if __name__ == "__main__":
    main()
