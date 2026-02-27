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
# 2) Notion API 配置
# =========================
NOTION_VERSION = "2025-09-03"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# =========================
# 3) 你的 Notion 数据库列名（必须和 Notion 完全一致）
#    - ticker: 你的第一列 title，列名就是 "ticker"
#    - 价格: Number
#    - 价格更新时间: Date
# =========================
TICKER_PROP_NAME = "ticker"
PRICE_PROP_NAME = "价格"
PRICE_TIME_PROP_NAME = "价格更新时间"


# =========================
# 4) 查询 Notion 数据库所有 pages（带分页）
# =========================
def notion_query_all_pages():
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    pages = []
    payload = {}

    while True:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=30)

        if r.status_code != 200:
            print("Notion query failed.")
            print("Status:", r.status_code)
            print("Response:", r.text)  # 这里会打印 Notion 的详细错误 JSON
            raise SystemExit(1)

        data = r.json()
        pages.extend(data.get("results", []))

        if data.get("has_more"):
            payload["start_cursor"] = data.get("next_cursor")
        else:
            break

    return pages


# =========================
# 5) 从每个 page 读取 ticker
#    支持：Title / Rich text / Select
# =========================
def get_ticker_from_page(page):
    prop = page["properties"].get(TICKER_PROP_NAME)
    if not prop:
        return None

    t = prop.get("type")

    # Title 类型（你的 ticker 第一列一般是 title）
    if t == "title":
        arr = prop.get("title", [])
        return arr[0]["plain_text"].strip() if arr else None

    # Rich text 类型
    if t == "rich_text":
        arr = prop.get("rich_text", [])
        return arr[0]["plain_text"].strip() if arr else None

    # Select 类型
    if t == "select":
        sel = prop.get("select")
        return sel["name"].strip() if sel else None

    return None


# =========================
# 6) 免费行情源：Stooq（无需 API Key）
#    美股符号格式：aapl.us
# =========================
def stooq_symbol(ticker: str) -> str:
    t = ticker.strip().lower()
    if t.endswith(".us"):
        return t
    return f"{t}.us"


def fetch_latest_close_from_stooq(ticker: str):
    """
    Stooq CSV:
      https://stooq.com/q/d/l/?s=aapl.us&i=d
    返回日线数据：Date,Open,High,Low,Close,Volume
    我们取最后一行作为最近交易日 close。
    """
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

    date_str = last[0]      # YYYY-MM-DD
    close_str = last[4]     # Close

    if close_str in ("", "nan", "NaN"):
        print(f"Stooq close is empty: {ticker}")
        return None

    try:
        return float(close_str), date_str
    except:
        print(f"Stooq close parse error: {ticker}, close={close_str}")
        return None


# =========================
# 7) 更新 Notion page：写入 价格 / 价格更新时间
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
        print(f"Notion update failed for page {page_id}")
        print("Status:", r.status_code)
        print("Response:", r.text)
        raise SystemExit(1)


# =========================
# 8) 主流程：读 ticker → 拉 close → 写回 Notion
# =========================
def main():
    print("Start: query notion database pages...")
    pages = notion_query_all_pages()
    print(f"Found pages: {len(pages)}")

    ok, fail, skip = 0, 0, 0

    for p in pages:
        page_id = p["id"]
        ticker = get_ticker_from_page(p)

        if not ticker:
            print(f"Skip (no ticker): {page_id}")
            skip += 1
            continue

        res = fetch_latest_close_from_stooq(ticker)
        if not res:
            print(f"Fail fetch: {ticker}")
            fail += 1
            continue

        close_price, close_date = res

        try:
            notion_update_page(page_id, close_price, close_date)
            print(f"OK {ticker} close={close_price} date={close_date}")
            ok += 1
            time.sleep(0.35)  # 轻微延迟，避免 Notion 限速
        except Exception as e:
            print(f"ERROR update {ticker}: {e}")
            fail += 1

    print(f"Done. ok={ok}, fail={fail}, skip={skip}")


if __name__ == "__main__":
    main()
