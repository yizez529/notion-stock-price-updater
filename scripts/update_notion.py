#!/usr/bin/env python3
"""
Notion Stock Price Updater
每日自动更新 Notion 股票数据库：收盘价、MA20/50/200、阶段、触发信号、更新时间

2026-07 修订说明：
- 修复"反转确认"（金叉）分支永远不可达的死代码 bug
- 突破/金叉类事件信号从"仅判断今天"改为"最近 N 个交易日窗口"，避免脚本停跑期间错过事件
- 回踩阈值从固定 2% 改为基于 ATR 的动态阈值（有上下限保护）
- 新增"放量"触发信号（相对前20日均量）
- 新增"数据日期"字段，与"价格更新时间"（脚本执行时间）区分开
- 写入 Notion 前先读取数据库实际字段结构，字段不存在时跳过而不是让整条更新失败
- yfinance 拉取增加重试
"""

import os
import sys
import json
import time
import requests
import yfinance as yf
import numpy as np
import pandas as pd
import math
from datetime import datetime, timezone

# ── 配置 ──────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2025-09-03"  # 新版 API，支持 data_sources
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# 事件类信号（突破/金叉）的回溯窗口：检测"最近 N 个交易日内"是否发生过穿越，
# 而不是只看"昨天 vs 今天"——避免 workflow 停跑期间事件被错过
EVENT_LOOKBACK_DAYS = 5

# 回踩阈值：基于 ATR 动态计算，ATR 不足时退回固定阈值
ATR_PERIOD = 14
ATR_PULLBACK_MULTIPLIER = 0.6
MIN_PULLBACK_THRESHOLD = 0.01   # 下限，避免低波动股票阈值过窄
MAX_PULLBACK_THRESHOLD = 0.05   # 上限，避免高波动股票阈值过宽
PULLBACK_THRESHOLD_FALLBACK = 0.02  # ATR 数据不足（新上市/数据不全）时的退回值

# 量能突增判定倍数（相对前20日均量，不含当日）
VOLUME_SURGE_MULTIPLIER = 1.5

# yfinance 拉取重试
FETCH_RETRIES = 2
FETCH_RETRY_BACKOFF_SEC = 3.0

# 记录已经警告过"数据库缺少此字段"的字段名，避免每个 ticker 都重复刷屏
_WARNED_MISSING_FIELDS: set[str] = set()


def finite_number(value) -> float | None:
    """Reject NaN, Infinity, and invalid numeric values before any Notion write."""
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


# ── Notion：获取 data_source_ids ──────────────────────
def get_data_source_ids() -> list[str]:
    """从数据库元数据中获取 data_source IDs（新版 Notion API 必需）"""
    resp = requests.get(
        f"{NOTION_API}/databases/{DATABASE_ID}",
        headers=HEADERS,
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"[FATAL] Notion retrieve database failed: {resp.status_code}")
        print(resp.text)
        sys.exit(1)

    data = resp.json()
    data_sources = data.get("data_sources", [])
    if not data_sources:
        print("[FATAL] No data_sources found in database. Is DATABASE_ID correct?")
        sys.exit(1)

    ids = [ds["id"] for ds in data_sources if ds.get("id")]
    print(f"[Notion] Found {len(ids)} data source(s)")
    return ids


# ── Notion：获取字段结构（用于避免写入不存在的字段）────────
def get_schema_properties(ds_ids: list[str]) -> set[str] | None:
    """获取所有 data_source 的字段名集合。获取失败时返回 None，
    调用方应退回到"全部尝试写入"的旧逻辑（不做字段过滤）。"""
    props: set[str] = set()
    ok = False
    for dsid in ds_ids:
        resp = requests.get(f"{NOTION_API}/data_sources/{dsid}", headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"[WARN] 无法获取 data_source {dsid[:8]}... 的字段结构（{resp.status_code}），跳过字段校验")
            continue
        data = resp.json()
        props.update(data.get("properties", {}).keys())
        ok = True
    return props if ok else None


# ── Notion：分页读取所有 ticker ──────────────────────────
def fetch_all_pages(ds_ids: list[str]) -> list[dict]:
    """通过 data_sources 端点分页读取所有页面"""
    all_pages = []

    for dsid in ds_ids:
        url = f"{NOTION_API}/data_sources/{dsid}/query"
        payload = {}
        while True:
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
            if resp.status_code != 200:
                print(f"[ERROR] Query data_source {dsid} failed: {resp.status_code}")
                print(resp.text)
                break
            data = resp.json()
            all_pages.extend(data.get("results", []))
            if data.get("has_more"):
                payload["start_cursor"] = data.get("next_cursor")
            else:
                break
        print(f"  data_source {dsid[:8]}...: {len(all_pages)} pages")

    print(f"[Notion] 共读取 {len(all_pages)} 条记录")
    return all_pages


def extract_ticker(page: dict) -> str | None:
    """从 Notion page 中提取 ticker 文本（兼容 title / rich_text）"""
    props = page.get("properties", {})
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


# ── 技术指标计算辅助函数 ──────────────────────────────
def compute_atr(hist, period: int = ATR_PERIOD):
    """真实波幅 ATR，用于动态回踩阈值"""
    high = hist["High"]
    low = hist["Low"]
    close = hist["Close"]
    prev_close = close.shift(1)
    tr = np.maximum(high - low, np.maximum((high - prev_close).abs(), (low - prev_close).abs()))
    return tr.rolling(period).mean()


def detect_recent_cross_up(fast, slow, lookback: int) -> bool:
    """检测 fast 线在最近 lookback 个交易日内是否上穿 slow 线。
    基于两条完整序列做逐日比较，而不是只看最新一天的静态大小关系，
    这样即使脚本中途停跑几天，重新跑起来时窗口内发生过的穿越依然能被捕捉到。"""
    if fast is None or slow is None:
        return False
    valid = fast.notna() & slow.notna()
    f = fast[valid]
    s = slow[valid]
    if len(f) < 2:
        return False
    crossed_up = (f.shift(1) <= s.shift(1)) & (f > s)
    return bool(crossed_up.tail(lookback).any())


# ── 股票数据获取 ──────────────────────────────────────
def fetch_stock_data(ticker: str) -> dict | None:
    """用 yfinance 获取历史数据并计算技术指标，带重试。"""
    last_err = None
    hist = None
    for attempt in range(FETCH_RETRIES + 1):
        try:
            tk = yf.Ticker(ticker)
            h = tk.history(period="1y")  # ~252 交易日，够算 MA200
            if h.empty or len(h) < 5:
                raise ValueError(f"历史数据不足（{len(h)} 条）")
            hist = h
            break
        except Exception as e:
            last_err = e
            if attempt < FETCH_RETRIES:
                print(f"  [WARN] {ticker}: 第{attempt + 1}次尝试失败（{e}），{FETCH_RETRY_BACKOFF_SEC}s后重试...")
                time.sleep(FETCH_RETRY_BACKOFF_SEC)

    if hist is None:
        print(f"  [ERROR] {ticker}: 重试{FETCH_RETRIES}次后仍失败 - {last_err}")
        return None

    try:
        # Yahoo data may contain non-finite quotes despite having enough rows.
        # In particular, never replace a bad latest close with a stale close.
        hist = hist.copy()
        if "Close" not in hist.columns:
            raise ValueError("行情缺少 Close 列")
        for col in ("Close", "High", "Low", "Volume"):
            if col in hist.columns:
                hist[col] = pd.to_numeric(hist[col], errors="coerce").replace(
                    [np.inf, -np.inf], np.nan
                )
        latest_close = finite_number(hist["Close"].iloc[-1])
        if latest_close is None or latest_close <= 0:
            raise ValueError("最新收盘价无效（NaN/Infinity/非正数），拒绝覆盖 Notion 旧价格")
        hist = hist.loc[hist["Close"].notna() & (hist["Close"] > 0)].copy()
        if len(hist) < 5:
            raise ValueError("清洗后有效行情少于 5 条")
        close = hist["Close"]
        volume = hist["Volume"] if "Volume" in hist.columns else None
        data_date = hist.index[-1].strftime("%Y-%m-%d")

        ma20_series = close.rolling(20).mean()
        ma50_series = close.rolling(50).mean()
        ma200_series = close.rolling(200).mean()

        ma20 = finite_number(ma20_series.iloc[-1]) if len(close) >= 20 else None
        ma50 = finite_number(ma50_series.iloc[-1]) if len(close) >= 50 else None
        ma200 = finite_number(ma200_series.iloc[-1]) if len(close) >= 200 else None

        # ATR 动态回踩阈值（相对价格的百分比），有上下限保护
        pullback_threshold = PULLBACK_THRESHOLD_FALLBACK
        if {"High", "Low"}.issubset(hist.columns):
            atr_series = compute_atr(hist)
            latest_atr = finite_number(atr_series.iloc[-1])
            if latest_atr is not None and latest_atr >= 0:
                atr_pct = latest_atr / latest_close
                pullback_threshold = min(
                    max(atr_pct * ATR_PULLBACK_MULTIPLIER, MIN_PULLBACK_THRESHOLD),
                    MAX_PULLBACK_THRESHOLD,
                )

        # 量能：相对前20日均量（不含当日）的倍数
        vol_ratio = None
        if volume is not None and len(volume) >= 21:
            avg_vol_prior = finite_number(volume.rolling(20).mean().shift(1).iloc[-1])
            latest_vol = finite_number(volume.iloc[-1])
            if avg_vol_prior is not None and avg_vol_prior > 0 and latest_vol is not None and latest_vol >= 0:
                vol_ratio = finite_number(latest_vol / avg_vol_prior)

        return {
            "close": latest_close,
            "data_date": data_date,
            "ma20": ma20,
            "ma50": ma50,
            "ma200": ma200,
            "pullback_threshold": pullback_threshold,
            "vol_ratio": vol_ratio,
            "broke_ma50_recent": detect_recent_cross_up(close, ma50_series, EVENT_LOOKBACK_DAYS),
            "broke_ma200_recent": detect_recent_cross_up(close, ma200_series, EVENT_LOOKBACK_DAYS),
            "golden_cross_recent": detect_recent_cross_up(ma50_series, ma200_series, EVENT_LOOKBACK_DAYS),
        }
    except Exception as e:
        print(f"  [ERROR] {ticker}: 计算指标失败 - {e}")
        return None


# ── 阶段与触发信号计算 ────────────────────────────────
def compute_triggers(data: dict) -> list[str]:
    """计算触发信号（多选），基于收盘价与均线的客观关系"""
    triggers = []
    c = data["close"]
    ma20 = data.get("ma20")
    ma50 = data.get("ma50")
    ma200 = data.get("ma200")
    threshold = data.get("pullback_threshold", PULLBACK_THRESHOLD_FALLBACK)

    # 排列类
    if ma20 and ma50 and ma200:
        if ma20 > ma50 > ma200:
            triggers.append("多头排列")
        elif ma20 < ma50 < ma200:
            triggers.append("空头排列")

    # 站上 / 跌破
    if ma50:
        triggers.append("站上MA50" if c > ma50 else "跌破MA50")
    if ma200:
        triggers.append("站上MA200" if c > ma200 else "跌破MA200")

    # 回踩类（动态 ATR 阈值）
    if ma20 and abs(c - ma20) / ma20 <= threshold:
        triggers.append("回踩MA20")
    if ma50 and abs(c - ma50) / ma50 <= threshold:
        triggers.append("回踩MA50")
    if ma200 and abs(c - ma200) / ma200 <= threshold:
        triggers.append("回踩MA200")

    # 放量
    vol_ratio = data.get("vol_ratio")
    if vol_ratio and vol_ratio >= VOLUME_SURGE_MULTIPLIER:
        triggers.append("放量")

    return triggers


def compute_stage(data: dict) -> str:
    """计算阶段（单选）。事件类信号（突破/金叉）优先级最高，
    且基于 EVENT_LOOKBACK_DAYS 窗口检测，而不是单日快照。"""
    c = data["close"]
    ma20 = data.get("ma20")
    ma50 = data.get("ma50")
    ma200 = data.get("ma200")
    threshold = data.get("pullback_threshold", PULLBACK_THRESHOLD_FALLBACK)

    # 如果均线数据不全，给一个基础判断
    if not ma50 or not ma200:
        if ma20 and c > ma20:
            return "数据不足（偏多）"
        return "数据不足"

    # ── 事件类（窗口内最近发生，优先级最高，因为是"事件"） ──
    if data.get("broke_ma200_recent"):
        return "突破MA200（拐点）"
    if data.get("broke_ma50_recent"):
        return "突破MA50（启动）"
    # 金叉（MA50 上穿 MA200）：修复原版逻辑顺序错误导致此分支永远不可达的 bug，
    # 现在提到"右侧上行"判断之前，作为独立事件类信号
    if data.get("golden_cross_recent") and c > ma50:
        return "反转确认"

    # ── 趋势类 ──
    if c > ma50 and c > ma200:
        if ma20 and abs(c - ma20) / ma20 <= threshold:
            return "突破回踩（健康）"
        if abs(c - ma50) / ma50 <= threshold:
            return "突破回踩（健康）"
        return "右侧上行"

    # ── 弱势类 ──
    if ma20 and c < ma20 and ma20 < ma50 < ma200:
        return "左侧下行"

    if c < ma200:
        if ma20 and c >= ma20:
            return "承压反弹"
        return "跌破MA200（走弱）"

    if c < ma50 and ma20 and c >= ma20:
        return "承压反弹"

    return "震荡整理"


# ── 更新 Notion 页面 ──────────────────────────────────
def update_page(page_id: str, ticker: str, data: dict, schema_props: set[str] | None = None):
    """将计算结果写回 Notion page。若提供 schema_props，会先过滤掉数据库里
    不存在的字段，避免因为单个字段缺失导致整条记录更新失败。"""
    # Validate the final Notion boundary independently of the data provider.
    data = dict(data)
    data["close"] = finite_number(data.get("close"))
    if data["close"] is None or data["close"] <= 0:
        print(f"  [FAIL] {ticker}: 无效收盘价，未写入 Notion")
        return False
    for name in ("ma20", "ma50", "ma200", "vol_ratio"):
        data[name] = finite_number(data.get(name))
    data["pullback_threshold"] = finite_number(data.get("pullback_threshold")) or PULLBACK_THRESHOLD_FALLBACK
    triggers = compute_triggers(data)
    stage = compute_stage(data)
    now_iso = datetime.now(timezone.utc).isoformat()

    desired = {
        "价格": {"number": round(data["close"], 2)},
        "价格更新时间": {"date": {"start": now_iso}},
        "数据日期": {"date": {"start": data["data_date"]}},
        "阶段": {"select": {"name": stage}},
        "触发信号": {"multi_select": [{"name": t} for t in triggers]},
    }

    # MA 字段：有值才更新，避免覆盖为 null 导致 Notion 报错
    if data.get("ma20") is not None:
        desired["MA20"] = {"number": round(data["ma20"], 2)}
    if data.get("ma50") is not None:
        desired["MA50"] = {"number": round(data["ma50"], 2)}
    if data.get("ma200") is not None:
        desired["MA200"] = {"number": round(data["ma200"], 2)}

    if schema_props is not None:
        missing = [k for k in desired if k not in schema_props]
        for m in missing:
            if m not in _WARNED_MISSING_FIELDS:
                print(f"[WARN] Notion 数据库缺少字段「{m}」，本次跳过写入该字段。如需启用，请在数据库中手动添加该字段（类型参考 README）。")
                _WARNED_MISSING_FIELDS.add(m)
        properties = {k: v for k, v in desired.items() if k not in missing}
    else:
        properties = desired

    try:
        # Prevent the requests library from rejecting a NaN at serialization time.
        json.dumps({"properties": properties}, allow_nan=False)
        resp = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=HEADERS,
            json={"properties": properties},
            timeout=30,
        )
    except (ValueError, TypeError, requests.RequestException) as exc:
        print(f"  [FAIL] {ticker}: Notion 请求/数据校验失败 - {exc}")
        return False

    if resp.status_code == 200:
        print(f"  [OK] {ticker}: 价格={data['close']:.2f}, 阶段={stage}, 信号={triggers}")
        return True
    try:
        error_detail = json.dumps(resp.json(), ensure_ascii=False)
    except ValueError:
        error_detail = resp.text[:500]
    print(f"  [FAIL] {ticker}: Notion API {resp.status_code} - {error_detail}")
    if resp.status_code == 400:
        print("  [DEBUG] 请检查 Notion 数据库是否包含以下字段：价格、MA20、MA50、MA200、阶段、触发信号、价格更新时间、数据日期")
    return False


# ── 主流程 ────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"Notion Stock Updater - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # 1) 获取 data_source 与字段结构
    ds_ids = get_data_source_ids()
    schema_props = get_schema_properties(ds_ids)
    if schema_props is None:
        print("[WARN] 未能获取字段结构，将尝试写入全部字段（如某字段不存在，单条记录会整体更新失败）")

    # 2) 读取所有 ticker
    pages = fetch_all_pages(ds_ids)
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

        print(f"\n>> 处理 {ticker} ...", flush=True)
        try:
            data = fetch_stock_data(ticker)
            if data and update_page(page["id"], ticker, data, schema_props):
                success += 1
            else:
                fail += 1
        except Exception as exc:
            # Failure of one ticker must not block the rest of the portfolio.
            print(f"  [FAIL] {ticker}: 未预期异常 - {type(exc).__name__}: {exc}")
            fail += 1
        finally:
            time.sleep(0.4)

    print("\n" + "=" * 60)
    print(f"完成: 成功={success}, 失败={fail}, 跳过={skip}, 总计={len(pages)}")
    print("=" * 60)

    if fail > 0:
        print(f"\n[WARN] 有 {fail} 个 ticker 更新失败，请检查上方日志")
        sys.exit(1)


if __name__ == "__main__":
    main()
