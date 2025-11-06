import os
import requests
import pandas as pd

# === 配置 ===
API_KEY = os.getenv("CRYPTOCOMPARE_API_KEY", "YOUR_API_KEY_HERE")  # 建议放到环境变量里
URL = "https://min-api.cryptocompare.com/data/all/coinlist"
OUT_ALL = "cryptocompare_all_coins.csv"
OUT_SYMBOLS = "cryptocompare_all_coins_symbols.csv"

def fetch_all_coins():
    params = {"api_key": API_KEY}
    headers = {"Accept": "application/json"}
    resp = requests.get(URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    # 基本校验
    if str(payload.get("Response", "")).lower() != "success":
        raise RuntimeError(f"API returned error: {payload.get('Message', payload)}")

    data = payload.get("Data", {})
    if not isinstance(data, dict) or not data:
        raise RuntimeError("Empty or invalid 'Data' in response")

    # Data 是以 Symbol 为键的 dict，值为 coin 字典
    coins = list(data.values())

    # 展开为 DataFrame
    df = pd.json_normalize(coins)

    # 规范常见列名（不同版本字段可能略有差异，尽量兼容）
    # 优先使用 'Symbol'，有些返回里也可能是 'Name'
    if "Symbol" not in df.columns and "Name" in df.columns:
        df = df.rename(columns={"Name": "Symbol"})
    if "CoinName" not in df.columns and "FullName" in df.columns:
        # 兼容：有时 FullName = "Bitcoin (BTC)"，CoinName = "Bitcoin"
        pass

    # 选择一组常用字段（保留不存在的列时会自动跳过）
    preferred_cols = [
        "Id", "Symbol", "CoinName", "FullName", "AssetTokenStatus",
        "Algorithm", "ProofType", "Rating.Weiss.Rating", "Rating.Weiss.TechnologyAdoptionRating",
        "BuiltOn", "SmartContractAddress", "PlatformType",
        "TotalCoinSupply", "MaxSupply", "IsTrading", "SortOrder",
        "Url", "ImageUrl"
    ]
    keep_cols = [c for c in preferred_cols if c in df.columns]
    if keep_cols:
        df = df[keep_cols]

    # 排序一下，优先按 Symbol
    if "Symbol" in df.columns:
        df = df.sort_values("Symbol")
    elif "FullName" in df.columns:
        df = df.sort_values("FullName")

    return df

def main():
    df = fetch_all_coins()
    # 保存所有字段
    df.to_csv(OUT_ALL, index=False, encoding="utf-8")
    print(f"Saved all coins to: {OUT_ALL} (rows={len(df)})")

    # 仅导出 Symbol 列
    if "Symbol" in df.columns:
        df_symbols = pd.DataFrame(sorted(df["Symbol"].dropna().unique()), columns=["Symbol"])
        df_symbols.to_csv(OUT_SYMBOLS, index=False, encoding="utf-8")
        print(f"Saved symbol list to: {OUT_SYMBOLS} (count={len(df_symbols)})")
    else:
        print("No 'Symbol' column found; skipped symbols CSV.")

if __name__ == "__main__":
    main()
