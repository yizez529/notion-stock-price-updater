import os
import time
import requests

# ========= 读取 Secrets =========
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
DATABASE_ID = os.environ.get("DATABASE_ID", "").strip()

if not NOTION_TOKEN:
    raise SystemExit("Missing NOTION_TOKEN (check GitHub Secrets).")
if not DATABASE_ID:
    raise SystemExit("Missing DATABASE_ID (check GitHub Secrets).")

# ========= Notion API（新版本：支持多数据源）=========
NOTION_VERSION = "2025-09-03"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ========= Notion 字段名（必须与你数据库完全一致）=========
TICKER_PROP = "ticker"          # Title 列名（你现在是小写 ticker）
PRICE_TIME_PROP = "价格更新时间"  # Date
PRICE_PROP = "价格"              # Number
MA20_PROP = "MA20"              # Number
MA50_PROP = "MA50"              # Number
MA200_PROP = "MA200"            # Number

# ✅ 新增：自动标签字段
STAGE_PROP = "阶段"              # Select
SIGNALS_PROP = "触发信号"         # Multi-select

# ========= 阈值（敏感版默认）=========
NEAR_BAND = 0.01      # 回踩/接近：±1%
BREAK_BUF = 0.005     # 站上确认：+0.5%
SLOPE_EPS = 0.001     # 均线斜率判定阈值：0.1%
SLOPE_LOOKBACK = 5    # 用 5 个交易日前对比判断斜率


def fail(prefix, r: requests.Response):
    print(prefix)
    print("Status:", r.status_code)
    print("Response:", r.text)
    raise SystemExit(1)


def above(price, ma):
    return ma is not None and price >= ma * (1 + BREAK_BUF)


def below(price, ma):
    return ma is not None and price <= ma * (1 - BREAK_BUF)


def near(price, ma):
    return ma is not None and abs(price - ma) / ma <= NEAR_BAND


# ======= Notion：先取 database 的 data_sources（关键）=======
def notion_get_data_source_ids(database_id: str):
    url = f"https://api.notion.com/v1/databases/{database_id}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        fail("Notion retrieve database failed.", r)

    data = r.json()
    data_sources = data.get("data_sources", [])
    if not data_sources:
        raise SystemExit("No data_sources found. Is this a database object?")

    ids = [ds["id"] for ds in data_sources if ds.get("id")]
    print("Found data_sources:", ids)
    return ids


# ======= Notion：查询 data source（替代 database query）=======
def notion_query_data_source_all_pages(data_source_id: str):
    url = f"https://api.notion.com/v1/data_sources/{data_source_id}/query"
    pages = []
    payload = {}

    while True:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        if r.status_code != 200:
            fail(f"Notion query data_source failed: {data_source_id}", r)

        data = r.json()
        pages.extend(data.get("results", []))

        if data.get("has_more"):
            payload["start_cursor"] = data.get("next_cursor")
        else:
            break

    return pages


def notion_query_all_pages():
    ds_ids = notion_get_data_source_ids(DATABASE_ID)
    all_pages = []
    for dsid in ds_ids:
        ps = notion_query_data_source_all_pages(dsid)
        print(f"Data source {dsid}: pages={len(ps)}")
        all_pages.extend(ps)
    return all_pages


def get_ticker(page):
    props = page.get("properties", {})
    prop = props.get(TICKER_PROP)
    if not prop:
        return None
    if prop.get("type") == "title":
        arr = prop.get("title", [])
        return arr[0]["plain_text"].strip() if arr else None
    return None


# ========= 免费行情：Stooq（日线）=========
def stooq_symbol(ticker: str) -> str:
    t = ticker.strip().lower()
    if t.endswith(".us"):
        return t
    return f"{t}.us"


def fetch_daily_closes_from_stooq(ticker: str):
    """
    返回按日期升序的 (date, close) 列表
    """
    sym = stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        print("Stooq fetch failed:", ticker, r.status_code)
        return None

    lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
    if len(lines) < 3:
        print("Stooq no enough data:", ticker)
        return None

    out = []
    for ln in lines[1:]:
        parts = ln.split(",")
        if len(parts) < 5:
            continue
        d = parts[0]
        c = parts[4]
        if not c or c.lower() == "nan":
            continue
        try:
            out.append((d, float(c)))
        except:
            continue

    return out if out else None


def sma(values, window: int):
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def ma_slope(values, window: int, lookback: int = SLOPE_LOOKBACK):
    """
    用 ma_today vs ma_(lookback天前) 判断斜率：up/down/flat
    """
    ma_today = sma(values, window)
    if ma_today is None or len(values) < window + lookback:
        return "flat"

    ma_prev = sma(values[:-lookback], window)
    if ma_prev is None or ma_prev == 0:
        return "flat"

    change = (ma_today - ma_prev) / ma_prev
    if change > SLOPE_EPS:
        return "up"
    if change < -SLOPE_EPS:
        return "down"
    return "flat"


def calc_signals(price, ma20, ma50, ma200, ma50_prev, ma200_prev, slopes, stacks):
    signals = []

    bull_stack, bear_stack = stacks
    slope20, slope50, slope200 = slopes

    if bull_stack:
        signals.append("多头排列")
    if bear_stack:
        signals.append("空头排列")

    if above(price, ma50):
        signals.append("站上MA50")
    if below(price, ma50):
        signals.append("跌破MA50")
    if above(price, ma200):
        signals.append("站上MA200")
    if below(price, ma200):
        signals.append("跌破MA200")

    if near(price, ma20):
        signals.append("回踩MA20")
    if near(price, ma50):
        signals.append("回踩MA50")

    # 金叉/死叉（需要昨天的 MA50/MA200）
    if (ma50 is not None and ma200 is not None and ma50_prev is not None and ma200_prev is not None):
        if ma50_prev <= ma200_prev and ma50 > ma200:
            signals.append("MA50上穿MA200")
        if ma50_prev >= ma200_prev and ma50 < ma200:
            signals.append("MA50下穿MA200")

    # 启动迹象：MA20拐头 + 价格站上MA20 + 贴近MA50（2%内）
    if ma20 is not None and ma50 is not None:
        if slope20 in ("up", "flat") and price > ma20 and abs(price - ma50) / ma50 <= 0.02 and price < ma50 * (1 + BREAK_BUF):
            signals.append("启动迹象")

    return signals


def calc_stage(price, ma20, ma50, ma200, slopes, stacks, signals):
    """
    9阶段（敏感版）- 有优先级（从强到弱）
    """
    bull_stack, bear_stack = stacks
    slope20, slope50, slope200 = slopes

    # 1) 右侧上行：多头排列 + 价格在MA20上 + 中长均线不下行
    if bull_stack and ma20 is not None and price > ma20 and slope50 != "down":
        return "右侧上行"

    # 2) 突破回踩：仍在MA50上方且贴近MA50（确认支撑）
    if ma50 is not None and price >= ma50 and near(price, ma50) and ("站上MA50" in signals):
        return "突破回踩"

    # 3) 突破MA200：收盘有效站上MA200
    if ma200 is not None and above(price, ma200):
        return "突破MA200"

    # 4) 突破MA50：收盘有效站上MA50
    if ma50 is not None and above(price, ma50):
        return "突破MA50"

    # 5) 准备启动：站上MA20 + MA20拐头 + 逼近MA50（2%内但未确认突破）
    if ma20 is not None and ma50 is not None:
        if price > ma20 and slope20 in ("up", "flat") and abs(price - ma50) / ma50 <= 0.02 and price < ma50 * (1 + BREAK_BUF):
            return "准备启动"

    # 6) 震荡：MA50附近来回（或均线缠绕）
    if ma50 is not None and abs(price - ma50) / ma50 <= 0.02:
        return "震荡"

    # 7) 承压反弹：MA50下方，但高于MA20（反弹中，仍承压）
    if ma20 is not None and ma50 is not None:
        if price < ma50 and price > ma20:
            return "承压反弹"

    # 8) 跌破MA200：深度弱势（放在左侧下行之前做显著提示）
    if ma200 is not None and below(price, ma200):
        return "跌破MA200"

    # 9) 左侧下行：空头排列 + 价格在MA20下
    if bear_stack and ma20 is not None and price < ma20:
        return "左侧下行"

    return "震荡"


def notion_update_page(page_id: str, price: float, date_str: str, ma20, ma50, ma200, stage: str, signals):
    url = f"https://api.notion.com/v1/pages/{page_id}"

    props = {
        PRICE_PROP: {"number": round(price, 2)},
        PRICE_TIME_PROP: {"date": {"start": date_str}},
        STAGE_PROP: {"select": {"name": stage}},
        SIGNALS_PROP: {"multi_select": [{"name": s} for s in signals]},
    }
    if ma20 is not None:
        props[MA20_PROP] = {"number": round(ma20, 2)}
    if ma50 is not None:
        props[MA50_PROP] = {"number": round(ma50, 2)}
    if ma200 is not None:
        props[MA200_PROP] = {"number": round(ma200, 2)}

    payload = {"properties": props}
    r = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
    if r.status_code != 200:
        fail(f"Notion update failed for page {page_id}", r)


def main():
    print("Start: query notion pages...")
    pages = notion_query_all_pages()
    print("Total pages:", len(pages))

    ok, fail_cnt, skip = 0, 0, 0
    for p in pages:
        page_id = p["id"]
        ticker = get_ticker(p)
        if not ticker:
            skip += 1
            continue

        closes = fetch_daily_closes_from_stooq(ticker)
        if not closes:
            print("No closes:", ticker)
            fail_cnt += 1
            continue

        close_values = [c for (_, c) in closes]
        last_date, last_close = closes[-1]

        ma20 = sma(close_values, 20)
        ma50 = sma(close_values, 50)
        ma200 = sma(close_values, 200)

        ma50_prev = sma(close_values[:-1], 50)
        ma200_prev = sma(close_values[:-1], 200)

        slope20 = ma_slope(close_values, 20)
        slope50 = ma_slope(close_values, 50)
        slope200 = ma_slope(close_values, 200)

        bull_stack = (ma20 is not None and ma50 is not None and ma200 is not None and ma20 > ma50 > ma200)
        bear_stack = (ma20 is not None and ma50 is not None and ma200 is not None and ma20 < ma50 < ma200)

        signals = calc_signals(
            last_close, ma20, ma50, ma200, ma50_prev, ma200_prev,
            slopes=(slope20, slope50, slope200),
            stacks=(bull_stack, bear_stack),
        )
        stage = calc_stage(
            last_close, ma20, ma50, ma200,
            slopes=(slope20, slope50, slope200),
            stacks=(bull_stack, bear_stack),
            signals=signals,
        )

        try:
            notion_update_page(page_id, last_close, last_date, ma20, ma50, ma200, stage, signals)
            print(f"OK {ticker} date={last_date} close={last_close} stage={stage} signals={signals}")
            ok += 1
            time.sleep(0.35)
        except Exception as e:
            print("Update error:", ticker, e)
            fail_cnt += 1

    print(f"Done. ok={ok}, fail={fail_cnt}, skip={skip}")


if __name__ == "__main__":
    main()
