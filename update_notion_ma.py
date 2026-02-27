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

# ========= Notion API（新版本）=========
NOTION_VERSION = "2025-09-03"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ========= Notion 字段名（与你的数据库完全一致）=========
TICKER_PROP = "ticker"
PRICE_TIME_PROP = "价格更新时间"
PRICE_PROP = "价格"
MA20_PROP = "MA20"
MA50_PROP = "MA50"
MA200_PROP = "MA200"


def fail(prefix, r: requests.Response):
    print(prefix)
    print("Status:", r.status_code)
    print("Response:", r.text)
    raise SystemExit(1)


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
    prop = page.get("properties", {}).get(TICKER_PROP)
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

    # header: Date,Open,High,Low,Close,Volume
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

    if not out:
        return None

    return out  # 升序（stooq 默认就是升序）


def sma(values, window: int):
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def notion_update_page(page_id: str, price: float, date_str: str, ma20, ma50, ma200):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    props = {
        PRICE_PROP: {"number": round(price, 2)},
        PRICE_TIME_PROP: {"date": {"start": date_str}},
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

        # 最近一个交易日
        last_date, last_close = closes[-1]
        close_values = [c for (_, c) in closes]

        ma20 = sma(close_values, 20)
        ma50 = sma(close_values, 50)
        ma200 = sma(close_values, 200)

        try:
            notion_update_page(page_id, last_close, last_date, ma20, ma50, ma200)
            print(f"OK {ticker} date={last_date} close={last_close} ma20={ma20} ma50={ma50} ma200={ma200}")
            ok += 1
            time.sleep(0.35)
        except Exception as e:
            print("Update error:", ticker, e)
            fail_cnt += 1

    print(f"Done. ok={ok}, fail={fail_cnt}, skip={skip}")


if __name__ == "__main__":
    main()
