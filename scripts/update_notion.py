#!/usr/bin/env python3
"""
Notion Stock Price Updater
每日自动更新 Notion 股票数据库：收盘价、MA20/50/200、阶段、触发信号、更新时间
"""

import os
import sys
import json
import time
import requests
import yfinance as yf
import numpy as np
from datetime import datetime, timezone

# ── 配置 ──────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# 回踩阈值：Close 距离 MA 在此百分比内视为"回踩"
PULLBACK_THRESHOLD = 0.02  # 2%


# ── Notion 分页读取所有 ticker ──────────────────────────
def fetch_all_pages() -> list[dict]:
    """分页读取 Notion 数据库所有页面（每次最多 100 条）"""
    pages = []
    start_cursor = None
    while True:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = requests.post(
            f"{NOTION_API}/databases/{DATABASE_ID}/query",
            headers=HEADERS,
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
    print(f"[Notion] 共读取 {len(pages)} 条记录")
    return pages


def extract_ticker(page: dict) -> str | None:
    """从 Notion page 中提取 ticker 文本（兼容 title / rich_text）"""
    props = page.get("properties", {})
    # 尝试常见字段名
    for field_name in ["ticker", "Ticker", "TICKER", "名称", "代码"]:
        prop = props.get(field_name)
        if not prop:
            continue
        ptype = prop.get("type")
        if ptype == "title":
            texts = prop.get("title", [])
            if texts:
                return texts[0].get("plain_text", "").strip().upper()
        elif ptype == "rich_text":
            texts = prop.get("rich_text", [])
            if texts:
                return texts[0].get("plain_text", "").strip().upper()
    return None


# ── 股票数据获取 ──────────────────────────────────────
def fetch_stock_data(ticker: str) -> dict | None:
    """用 yfinance 获取收盘价 + 计算 MA20/50/200，返回 dict 或 None"""
    try:
        tk = yf.Ticker(ticker)
        # 拉 300 个交易日数据，确保 MA200 有足够数据
        hist = tk.history(period="1y")  # ~252 trading days
        if hist.empty or len(hist) < 5:
            print(f"  [WARN] {ticker}: 历史数据不足 ({len(hist)} 条)")
            return None

        close = hist["Close"]
        latest_close = float(close.iloc[-1])

        # 计算 MA
        ma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else None
        ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None

        # 昨日收盘（用于判断"突破"类信号）
        prev_close = float(close.iloc[-2]) if len(close) >= 2 else latest_close

        return {
            "close": latest_close,
            "prev_close": prev_close,
            "ma20": ma20,
            "ma50": ma50,
            "ma200": ma200,
        }
    except Exception as e:
        print(f"  [ERROR] {ticker}: 获取数据失败 - {e}")
        return None


# ── 阶段与触发信号计算 ────────────────────────────────
def compute_triggers(data: dict) -> list[str]:
    """计算触发信号（多选），基于收盘价与均线的客观关系"""
    triggers = []
    c = data["close"]
    ma20 = data.get("ma20")
    ma50 = data.get("ma50")
    ma200 = data.get("ma200")

    # 排列类
    if ma20 and ma50 and ma200:
        if ma20 > ma50 > ma200:
            triggers.append("多头排列")
        elif ma20 < ma50 < ma200:
            triggers.append("空头排列")

    # 站上 / 跌破
    if ma50:
        if c > ma50:
            triggers.append("站上MA50")
        else:
            triggers.append("跌破MA50")
    if ma200:
        if c > ma200:
            triggers.append("站上MA200")
        else:
            triggers.append("跌破MA200")

    # 回踩类（Close 距离 MA 在阈值内）
    if ma20 and abs(c - ma20) / ma20 <= PULLBACK_THRESHOLD:
        triggers.append("回踩MA20")
    if ma50 and abs(c - ma50) / ma50 <= PULLBACK_THRESHOLD:
        triggers.append("回踩MA50")
    if ma200 and abs(c - ma200) / ma200 <= PULLBACK_THRESHOLD:
        triggers.append("回踩MA200")

    return triggers


def compute_stage(data: dict) -> str:
    """计算阶段（单选），敏感版 8-10 标签，优先级从上到下"""
    c = data["close"]
    prev = data["prev_close"]
    ma20 = data.get("ma20")
    ma50 = data.get("ma50")
    ma200 = data.get("ma200")

    # 如果均线数据不全，给一个基础判断
    if not ma50 or not ma200:
        if ma20 and c > ma20:
            return "数据不足（偏多）"
        return "数据不足"

    # ── 突破类（优先判断，因为是"事件"） ──
    # 突破 MA200（拐点）：昨日 ≤ MA200，今日 > MA200
    if prev <= ma200 and c > ma200:
        return "突破MA200（拐点）"

    # 突破 MA50（启动）：昨日 ≤ MA50，今日 > MA50
    if prev <= ma50 and c > ma50:
        return "突破MA50（启动）"

    # ── 趋势类 ──
    # 右侧上行：Close > MA50 且 > MA200
    if c > ma50 and c > ma200:
        # 进一步区分：是否在回踩
        if ma20 and abs(c - ma20) / ma20 <= PULLBACK_THRESHOLD:
            return "突破回踩（健康）"
        if ma20 and abs(c - ma50) / ma50 <= PULLBACK_THRESHOLD:
            return "突破回踩（健康）"
        return "右侧上行"

    # 反转确认：Close > MA50 且 MA50 上穿 MA200（金叉）
    if ma20 and c > ma50 and ma50 > ma200:
        return "反转确认"

    # ── 弱势类 ──
    # 左侧下行：Close < MA20 且 MA20 < MA50 < MA200
    if ma20 and c < ma20 and ma20 < ma50 < ma200:
        return "左侧下行"

    # 跌破 MA200（走弱）
    if c < ma200:
        # 承压反弹：虽在 MA200 下方，但回到 MA20 附近/上方
        if ma20 and c >= ma20:
            return "承压反弹"
        return "跌破MA200（走弱）"

    # 承压反弹：Close < MA50 但 > MA20
    if c < ma50 and ma20 and c >= ma20:
        return "承压反弹"

    # 震荡整理：其余情况
    return "震荡整理"


# ── 更新 Notion 页面 ──────────────────────────────────
def update_page(page_id: str, ticker: str, data: dict):
    """将计算结果写回 Notion page"""
    triggers = compute_triggers(data)
    stage = compute_stage(data)
    now_iso = datetime.now(timezone.utc).isoformat()

    properties = {
        "价格": {"number": round(data["close"], 2)},
        "价格更新时间": {"date": {"start": now_iso}},
        "阶段": {"select": {"name": stage}},
        "触发信号": {"multi_select": [{"name": t} for t in triggers]},
    }

    # MA 字段：有值才更新，避免覆盖为 null 导致 Notion 报错
    if data["ma20"] is not None:
        properties["MA20"] = {"number": round(data["ma20"], 2)}
    if data["ma50"] is not None:
        properties["MA50"] = {"number": round(data["ma50"], 2)}
    if data["ma200"] is not None:
        properties["MA200"] = {"number": round(data["ma200"], 2)}

    resp = requests.patch(
        f"{NOTION_API}/pages/{page_id}",
        headers=HEADERS,
        json={"properties": properties},
    )

    if resp.status_code == 200:
        print(f"  [OK] {ticker}: 价格={data['close']:.2f}, 阶段={stage}, 信号={triggers}")
    else:
        err = resp.json()
        print(f"  [FAIL] {ticker}: Notion API {resp.status_code} - {json.dumps(err, ensure_ascii=False)}")
        # 如果是字段名不匹配，打印详细信息帮助排查
        if resp.status_code == 400:
            print(f"  [DEBUG] 请检查 Notion 数据库是否包含以下字段：价格、MA20、MA50、MA200、阶段、触发信号、价格更新时间")


# ── 主流程 ────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"Notion Stock Updater - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # 1) 读取所有 ticker
    pages = fetch_all_pages()
    if not pages:
        print("[WARN] 数据库为空，无 ticker 需要更新")
        return

    success = 0
    fail = 0
    skip = 0

    for page in pages:
        ticker = extract_ticker(page)
        if not ticker:
            print(f"  [SKIP] page {page['id'][:8]}... 无法提取 ticker")
            skip += 1
            continue

        print(f"\n>> 处理 {ticker} ...")
        data = fetch_stock_data(ticker)
        if not data:
            fail += 1
            continue

        update_page(page["id"], ticker, data)
        success += 1

        # 礼貌间隔，避免触发 Notion rate limit
        time.sleep(0.4)

    print("\n" + "=" * 60)
    print(f"完成: 成功={success}, 失败={fail}, 跳过={skip}, 总计={len(pages)}")
    print("=" * 60)

    if fail > 0:
        print(f"\n[WARN] 有 {fail} 个 ticker 更新失败，请检查上方日志")
        sys.exit(1)


if __name__ == "__main__":
    main()
