# Notion Stock Price Updater

每日自动更新 Notion 股票数据库：收盘价、MA20/50/200、阶段（8档敏感版）、触发信号。

## 架构

```
GitHub Actions (cron 周一~五 UTC 22:00 / 北京 06:00)
  → yfinance 拉收盘价 + 历史数据
  → 计算 MA20 / MA50 / MA200
  → 判定阶段 + 触发信号
  → Notion API 写回数据库
```

## Notion 数据库字段要求

| 字段名 | 类型 | 说明 |
|--------|------|------|
| ticker | Title 或 Text | 股票代码，如 AAPL、MSFT |
| 价格 | Number | 收盘价 |
| MA20 | Number | 20日均线 |
| MA50 | Number | 50日均线 |
| MA200 | Number | 200日均线 |
| 阶段 | Select | 8档自动判定 |
| 触发信号 | Multi-select | 客观信号组合 |
| 价格更新时间 | Date | 最后更新时间 |

## 阶段标签（敏感版 8 档）

1. **突破MA200（拐点）** — 昨日≤MA200，今日突破
2. **突破MA50（启动）** — 昨日≤MA50，今日突破
3. **右侧上行** — Close > MA50 且 > MA200
4. **突破回踩（健康）** — 上方趋势中回踩均线
5. **反转确认** — Close > MA50 且 MA50 > MA200（金叉）
6. **震荡整理** — 无明显趋势
7. **承压反弹** — 弱势中短线反弹到 MA20
8. **左侧下行** — Close < MA20，空头排列
9. **跌破MA200（走弱）** — 长期趋势转弱

## 触发信号集合

- 多头排列 / 空头排列
- 站上MA50 / 站上MA200
- 跌破MA50 / 跌破MA200
- 回踩MA20 / 回踩MA50 / 回踩MA200

## GitHub Secrets

| Secret 名 | 说明 |
|-----------|------|
| `NOTION_TOKEN` | Notion Integration Token |
| `DATABASE_ID` | Notion 数据库 ID |

## 手动触发

Actions → Update Notion Stock Prices → Run workflow
