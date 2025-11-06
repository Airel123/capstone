# cmc_snapshot_selenium_min_robust.py
import csv
import os
import random
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Set, Tuple
from contextlib import suppress
from pathlib import Path

import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ============ 可配置 ============
HEADLESS = True                  # 无头浏览
CAP_THRESHOLD = 100_000_000      # 仅保留 >= $100M
OPEN_TIMEOUT = 30                # 打开页面最大等待(s)
SCROLL_STEPS = 8                 # 触发懒加载的滚动步数（不需要可设为 0）
REQUESTS_PER_MIN = 4             # 日期之间的访问频率（速率限制）
START_DATE = "2021-01-02"
END_DATE   = "2024-12-30"
OUTFILE    = "cmc_snapshot_over_100m_selenium.csv"

# 断点与错误日志
CKPT_FILE  = OUTFILE + ".ckpt"   # 已成功抓取的 yyyymmdd 列表
ERR_FILE   = OUTFILE + ".errors.csv"  # 失败日志
# ==============================

BASE = "https://coinmarketcap.com/historical/{}/"
MONEY_RE = re.compile(r"[^\d.\-]")

# ---------- Windows 上自动获取 Chrome 主版本（解决 driver 不匹配） ----------
def _get_chrome_major_on_windows() -> Optional[int]:
    try:
        import winreg, re as _re
        paths = [
            r"SOFTWARE\Google\Chrome\BLBeacon",
            r"SOFTWARE\WOW6432Node\Google\Chrome\BLBeacon",
        ]
        for root in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
            for p in paths:
                try:
                    with winreg.OpenKey(root, p) as key:
                        v, _ = winreg.QueryValueEx(key, "version")
                        m = _re.match(r"(\d+)\.", v)
                        if m:
                            return int(m.group(1))
                except OSError:
                    pass
    except Exception:
        pass
    return None

# ---------- 驱动（抑制 WinError 6 的 __del__ 噪音） ----------
class SafeChrome(uc.Chrome):
    def __del__(self):
        with suppress(Exception):
            super().__del__()

def make_driver(headless: bool = True):
    opts = uc.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument("--lang=en-US,en;q=0.9")

    kwargs = {}
    major = _get_chrome_major_on_windows()
    if major:
        kwargs["version_main"] = major  # 匹配本机 Chrome 主版本

    driver = SafeChrome(options=opts, **kwargs)
    driver.set_page_load_timeout(OPEN_TIMEOUT)
    return driver

# ---------- 工具 ----------
def daterange(start: str, end: str) -> Iterable[str]:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    while s <= e:
        yield s.strftime("%Y%m%d")
        s += timedelta(days=1)

def gentle_scroll(driver, steps: int = SCROLL_STEPS, pause: float = 0.25):
    for _ in range(max(0, steps)):
        with suppress(Exception):
            driver.execute_script("window.scrollBy(0, 900);")
        time.sleep(pause + random.uniform(0.05, 0.12))

def parse_money_to_float(text: str) -> Optional[float]:
    if not text:
        return None
    t = text.strip()
    if t in {"", "—", "-", "–"}:
        return None
    cleaned = MONEY_RE.sub("", t)
    try:
        # 注意：这里假定页面给的是完整数字（非 1.2B/M 缩写）
        return float(cleaned) if cleaned else None
    except ValueError:
        return None

# ---------- 解析 ----------
@dataclass
class SnapRow:
    date: str
    rank: int
    name: str
    symbol: str
    market_cap_usd: float

TABLE_ROW_SELECTORS = [
    "tr.cmc-table-row",               # 旧样式
    "tbody tr:has(td.cmc-table__cell--sort-by__rank)",  # 回退
    "tbody tr",                       # 最宽松回退
]

def _find_table_rows(html: str):
    soup = BeautifulSoup(html, "lxml")
    for sel in TABLE_ROW_SELECTORS:
        rows = soup.select(sel)
        if rows:
            return soup, rows
    return soup, []

def extract_rows_from_html(html: str, date_yyyymmdd: str) -> List[SnapRow]:
    soup, trs = _find_table_rows(html)
    out: List[SnapRow] = []
    date_str = datetime.strptime(date_yyyymmdd, "%Y%m%d").strftime("%Y-%m-%d")

    for tr in trs:
        try:
            # rank
            rcell = tr.select_one("td.cmc-table__cell--sort-by__rank") or tr.find("td")
            rtxt = rcell.get_text(strip=True) if rcell else ""
            if not rtxt.isdigit():
                continue
            rank = int(rtxt)

            # symbol
            scell = tr.select_one("td.cmc-table__cell--sort-by__symbol")
            symbol = scell.get_text(strip=True) if scell else ""
            if not symbol:
                a = tr.select_one(".cmc-table__column-name--symbol")
                if a:
                    symbol = a.get_text(strip=True)

            # name
            name = ""
            ncell = tr.select_one("td.cmc-table__cell--sort-by__name")
            if ncell:
                a = ncell.select_one(".cmc-table__column-name--name")
                if a:
                    name = a.get_text(strip=True)
                else:
                    img = ncell.find("img")
                    if img and img.get("alt"):
                        name = img["alt"].strip()
                    else:
                        name = ncell.get_text(" ", strip=True)
            else:
                # 兜底：第一列文本
                tds = tr.find_all("td")
                if tds:
                    name = tds[0].get_text(" ", strip=True)

            # market cap
            mcell = tr.select_one("td.cmc-table__cell--sort-by__market-cap")
            cap_text = mcell.get_text(strip=True) if mcell else ""
            cap = parse_money_to_float(cap_text)

            if not name or not symbol or cap is None or cap < CAP_THRESHOLD:
                continue

            out.append(SnapRow(
                date=date_str, rank=rank, name=name, symbol=symbol, market_cap_usd=cap
            ))
        except Exception:
            # 某一行解析失败，跳过该行，保证“行级容错”
            continue

    # 去重（防 rank 连号/重复块）
    seen: Set[Tuple[str, int, float]] = set()
    uniq: List[SnapRow] = []
    for r in out:
        key = (r.symbol.upper(), r.rank, round(r.market_cap_usd, 2))
        if key not in seen:
            seen.add(key)
            uniq.append(r)
    return uniq

# ---------- 单日抓取（带重试） ----------
def wait_for_table(driver) -> None:
    # 等待任一选择器出现（presence）
    end_time = time.time() + OPEN_TIMEOUT
    poll = 0.5
    while time.time() < end_time:
        for sel in TABLE_ROW_SELECTORS:
            try:
                WebDriverWait(driver, poll).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                )
                return
            except Exception:
                pass
        time.sleep(poll)
    raise TimeoutException("Table not found within timeout")

def scrape_one_day_with_retry(driver, yyyymmdd: str, max_retry: int = 3) -> List[SnapRow]:
    url = BASE.format(yyyymmdd)
    attempt = 0
    backoff = 2.0
    last_exc = None

    while attempt < max_retry:
        attempt += 1
        try:
            driver.get(url)
            wait_for_table(driver)
            gentle_scroll(driver, steps=SCROLL_STEPS, pause=0.25)
            html = driver.page_source
            rows = extract_rows_from_html(html, yyyymmdd)
            return rows
        except (TimeoutException, WebDriverException) as e:
            last_exc = e
            # 停止加载，退避后重试
            with suppress(Exception):
                driver.execute_script("window.stop();")
            sleep_sec = backoff + random.uniform(0.2, 0.8)
            print(f"[{yyyymmdd}] attempt {attempt}/{max_retry} failed: {e.__class__.__name__}. Backoff {sleep_sec:.1f}s")
            time.sleep(sleep_sec)
            backoff *= 2
            continue
        except Exception as e:
            # 其他异常直接退避继续下次重试
            last_exc = e
            sleep_sec = backoff + random.uniform(0.2, 0.8)
            print(f"[{yyyymmdd}] attempt {attempt}/{max_retry} unknown error: {e}. Backoff {sleep_sec:.1f}s")
            time.sleep(sleep_sec)
            backoff *= 2
            continue

    # 全部失败，抛给上层记录错误
    raise last_exc if last_exc else RuntimeError("Unknown error")

# ---------- 增量写入 ----------
FIELDNAMES = ["date", "rank", "name", "symbol", "market_cap_usd"]

def ensure_csv_header(path: str):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES)
            w.writeheader()

def append_csv(path: str, rows: List[SnapRow]):
    if not rows:
        return
    ensure_csv_header(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        for r in sorted(rows, key=lambda r: (r.date, r.rank, r.symbol)):
            w.writerow(asdict(r))

def log_error(day: str, err: str):
    new_file = not Path(ERR_FILE).exists()
    with open(ERR_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["date", "error"])
        w.writerow([day, err])

# ---------- 断点续爬 ----------
def load_ckpt() -> Set[str]:
    done = set()
    p = Path(CKPT_FILE)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    done.add(line)
    return done

def append_ckpt(yyyymmdd: str):
    with open(CKPT_FILE, "a", encoding="utf-8") as f:
        f.write(yyyymmdd + "\n")

# ---------- 主流程 ----------
def main():
    driver = make_driver(HEADLESS)
    min_gap = 60.0 / max(1, REQUESTS_PER_MIN)
    last_ts = 0.0

    done_days = load_ckpt()
    print(f"Loaded checkpoint: {len(done_days)} days already done")

    try:
        for day in daterange(START_DATE, END_DATE):
            if day in done_days:
                print(f"{day}: skipped (in checkpoint)")
                continue

            # 频控 + 抖动
            wait = max(0.0, min_gap - (time.time() - last_ts)) + random.uniform(0.15, 0.45)
            time.sleep(wait)

            try:
                rows = scrape_one_day_with_retry(driver, day, max_retry=3)
                last_ts = time.time()
                print(f"{day}: {len(rows)} rows (>= $100M)")
                append_csv(OUTFILE, rows)
                append_ckpt(day)  # 只要成功执行到这里（即使 0 行）也记为完成
            except Exception as e:
                last_ts = time.time()
                msg = f"{type(e).__name__}: {str(e)}"
                print(f"{day}: FAILED -> {msg}")
                log_error(day, msg)
                # 不抛异常，继续下个日期

    finally:
        with suppress(Exception):
            driver.close()
        with suppress(Exception):
            driver.quit()

    print("Done. Incremental results written to:", OUTFILE)
    if Path(ERR_FILE).exists():
        print("Errors logged to:", ERR_FILE)
    print("Checkpoint file:", CKPT_FILE)

if __name__ == "__main__":
    main()
