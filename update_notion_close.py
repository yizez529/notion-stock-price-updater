import os
import time
import requests

# =========================
# 1) 从 GitHub Actions Secrets 读取
# =========================
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
DATABASE_ID = os.environ.get("DATABASE_ID", "").strip()

if not NOTION_TOKEN:
    raise SystemExit("Missing env NOTION_TOKEN (check GitHub Secrets).")
if not DATABASE_ID:
    raise SystemExit("Missing env DATABASE_ID (check GitHub Secrets).")

# =========================
# 2) Notion API 配置（新版必须 2025-09-03）
# =========================
NOTION_VERSION = "2025-09-03"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# =========================
# 3) 你的 Notion 数据库列名（必须完全一致）
# =========================
TICKER_PROP_NAME = "ticker"      # title 列名
PRICE_PROP_NAME = "价格"          # number
PRICE_TIME_PROP_NAME = "价格更新时间"  # date


def _fail(prefix: str, r: requests.Response):
    print(prefix)
    print("Status:", r.status_code)
    print("Response:", r.text)
    raise SystemExit(1)


# =========================
# 4) 先 retrieve database，拿到 data_sources（新版关键）
# =========================
def notion_get_data_source_ids(database_id: str):
    url = f"https://api.notion.com/v1/databases/{database_id}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        _fail("Notion retrieve database failed.", r)

    data = r.json()
    data_sources = data.get("data_sources", [])
    if not data_sources:
        raise SystemExit("No data_sources found in this database. (unexpected)")

    ids = [ds["id"] for ds in data_sources if ds.get("id")]
    print(f"Found data_sources: {len(ids)}")
    return ids


# =========================
# 5) Query a data source（替代旧的 database query）
# =========================
def notion_query_data_source_all_pages(data_source_id: str):
    url = f"https://api.notion.com/v1/data_sources/{data_source_id}/query"
    pages = []
    payload = {}

    while True:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        if r.status_code != 200:
            _fail(f"Notion query data_source failed: {data_source_id}", r)

        data = r.json()
        pages.extend(data.get("results", []))

        if data.get("has_more"):
            payload["start_cursor"] = data.get("next_cursor")
        else:
            break

    return pages


def notion_query_all_pages():
    # 1) 发现 data_source_ids
    data_source_ids = notion_get_data_source_ids(DATABASE_ID)

    # 2) 把所有 data source 的页面合并（你的库里可能有多个数据源）
    all_pages = []
    for dsid in data_source_ids:
        ds_pages = notion_query_data_source_all_pages(dsid)
        print(f"Data source {dsid}: pages={len(ds_pages)}")
        all_pages.extend(ds_pages)

    return all_pages


# =========================
# 6) 从每个 page 读取 ticker（支持 Title/Rich_text/Select）
# =========================
def get_ticker_from_page(page):
    props = page.get("properties", {})
    prop = props.get(TICKER_PROP_NAME)
    if not prop:
        return None

    t = prop.get("type")

    if t == "title":
        arr = prop.get("title", [])
        return arr[0]["plain_text"].strip() if arr else None

    if t == "rich_text":
        arr = prop.get("rich_text", [])
        return arr[0]["plain_text"].strip() if arr else None

    if t == "select":
        sel = prop.get("select")
        return sel["name"].strip() if sel else None

    return None


# =========================
# 7) 免费行情源：Stooq（无需 API Key）
# =========================
def stooq_symbol(ticker: str) -> str:
    t = ticker.strip().lower()
    if t.endswith(".us"):
        return t
    return f"{t}.us"


def fetch_latest_close_from_stooq(ticker: str):
    sym = stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"

    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        print(f"Stooq fetch failed: {ticker} status={r.status_code}")
        return None

    lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
    if len(lines) < 2:
        print(f"Stooq returned no data: {ticker}")
        return None

    last = lines[-1].split(",")
    if len(last) < 5:
        print(f"Stooq bad format: {ticker}")
        return None

    date_str = last[0]
    close_str = last[4]

    if close_str in ("", "nan", "NaN"):
        print(f"Stooq close is empty: {ticker}")
        return None

    try:
        return float(close_str), date_str
    except:
        print(f"Stooq close parse error: {ticker}, close={close_str}")
        return None


# =========================
# 8) 更新 Notion page：写入 价格 / 价格更新时间
# =========================
def notion_update_page(page_id: str, close_price: float, close_date: str):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            PRICE_PROP_NAME: {"number": close_price},
            PRICE_TIME_PROP_NAME: {"date": {"start": close_date}},
        }
    }

    r = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
    if r.status_code != 200:
        _fail(f"Notion update failed for page {page_id}", r)


def main():
    print("Start: query notion pages from data sources...")
    pages = notion_query_all_pages()
    print(f"Total pages fetched: {len(pages)}")

    ok, fail, skip = 0, 0, 0

    for p in pages:
        page_id = p["id"]
        ticker = get_ticker_from_page(p)

        if not ticker:
            skip += 1
            continue

        res = fetch_latest_close_from_stooq(ticker)
        if not res:
            fail += 1
            continue

        close_price, close_date = res

        try:
            notion_update_page(page_id, close_price, close_date)
            print(f"OK {ticker} close={close_price} date={close_date}")
            ok += 1
            time.sleep(0.35)
        except Exception as e:
            print(f"ERROR update {ticker}: {e}")
            fail += 1

    print(f"Done. ok={ok}, fail={fail}, skip={skip}")


if __name__ == "__main__":
    main()
