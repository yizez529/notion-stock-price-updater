import os, time, requests
from datetime import datetime, timezone

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]

NOTION_VERSION = "2022-06-28"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ✅ 这里填你的 ticker 列名（非常重要）
TICKER_PROP_NAME = "ticker"   # <-- 如果你的列名不是 ticker，就改成你实际列名

PRICE_PROP_NAME = "价格"
PRICE_TIME_PROP_NAME = "价格更新时间"


def notion_query_all_pages():
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    pages = []
    payload = {}
    while True:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        pages.extend(data["results"])
        if data.get("has_more"):
            payload["start_cursor"] = data["next_cursor"]
        else:
            break
    return pages


def get_ticker_from_page(page):
    prop = page["properties"].get(TICKER_PROP_NAME)
    if not prop:
        return None

    t = prop["type"]
    if t == "title":
        arr = prop["title"]
        return arr[0]["plain_text"].strip() if arr else None
    if t == "rich_text":
        arr = prop["rich_text"]
        return arr[0]["plain_text"].strip() if arr else None
    if t == "select":
        sel = prop["select"]
        return sel["name"].strip() if sel else None

    return None


def stooq_symbol(ticker: str) -> str:
    """
    Stooq 美股符号格式通常是: aapl.us
    这里统一转成小写 + .us
    """
    t = ticker.strip().lower()
    # 如果用户已经写了 .us 就不重复加
    if t.endswith(".us"):
        return t
    return f"{t}.us"


def fetch_latest_close_from_stooq(ticker: str):
    """
    Stooq 提供免费 CSV：
    https://stooq.com/q/d/l/?s=aapl.us&i=d
    返回日线数据，我们取最后一行（最近交易日）
    """
    sym = stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    # header: Date,Open,High,Low,Close,Volume
    last = lines[-1].split(",")
    if len(last) < 5:
        return None

    date_str = last[0]
    close_str = last[4]
    if close_str in ("", "nan", "NaN"):
        return None

    return float(close_str), date_str


def notion_update_page(page_id: str, close_price: float, close_date: str):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            PRICE_PROP_NAME: {"number": close_price},
            PRICE_TIME_PROP_NAME: {"date": {"start": close_date}},
        }
    }
    r = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()


def main():
    pages = notion_query_all_pages()
    print(f"Found pages: {len(pages)}")

    ok, fail = 0, 0
    for p in pages:
        page_id = p["id"]
        ticker = get_ticker_from_page(p)
        if not ticker:
            print(f"Skip (no ticker): {page_id}")
            continue

        try:
            res = fetch_latest_close_from_stooq(ticker)
            if not res:
                print(f"Fail fetch: {ticker}")
                fail += 1
                continue
            close_price, close_date = res
            notion_update_page(page_id, close_price, close_date)
            print(f"OK {ticker} close={close_price} date={close_date}")
            ok += 1
            time.sleep(0.35)  # 轻微延迟，避免限速
        except Exception as e:
            print(f"ERROR {ticker}: {e}")
            fail += 1

    print(f"Done. ok={ok}, fail={fail}")


if __name__ == "__main__":
    main()
