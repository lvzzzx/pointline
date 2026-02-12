# Tushare 数据整合方案

## 调研摘要

### 当前状态
项目已初步集成 Tushare，仅支持基础数据中的 `stock_basic` 接口：
- ✅ `stock_basic` - 股票基础列表（已实现）
- ✅ `dim_symbol` 同步（已实现）

### Tushare 数据能力概览

Tushare Pro 提供以下数据类别：

| 数据类别 | 主要接口 | 积分要求 | 更新频率 |
|---------|---------|---------|---------|
| **基础数据** | stock_basic, trade_cal, stock_company | 2000+ | 每日 |
| **行情数据** | daily, weekly, monthly, pro_bar(分钟) | 120-2000+ | 交易日15-17点 |
| **财务数据** | fina_indicator, income, balance_sheet, cashflow | 2000-5000+ | 季报发布时 |
| **市场数据** | daily_basic, moneyflow, limit_list | 2000+ | 每日 |
| **参考数据** | concept, industry, hk_hold | 2000+ | 每日/每周 |

---

## Phase 1: 行情数据 (Market Data)

### 1.1 日线行情 (daily)

**接口**: `daily` - A股日线行情（未复权）

**输入参数**:
- `ts_code`: 股票代码（支持多值，逗号分隔）
- `trade_date`: 交易日期（YYYYMMDD）
- `start_date/end_date`: 日期范围

**输出字段**:
```
ts_code, trade_date, open, high, low, close, pre_close,
change, pct_chg, vol, amount
```

**建议数据表**: `stock_daily_cn`

**Schema 设计**:
```python
STOCK_DAILY_CN_SCHEMA = {
    # 分区键
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,

    # 时间戳
    "ts_start_us": pl.Int64,  # 交易日开始 (09:30:00)
    "ts_end_us": pl.Int64,    # 交易日结束 (15:00:00)

    # 标识
    "symbol_id": pl.Int64,
    "exchange_symbol": pl.Utf8,

    # OHLC (fixed-point, 价格*10000)
    "open_px_int": pl.Int64,
    "high_px_int": pl.Int64,
    "low_px_int": pl.Int64,
    "close_px_int": pl.Int64,
    "pre_close_px_int": pl.Int64,

    # 涨跌幅
    "change_px_int": pl.Int64,
    "pct_chg": pl.Float64,  # 涨跌幅%

    # 成交量额
    "volume_qty_int": pl.Int64,     # 成交量（股，原始数据是手）
    "amount_quote_int": pl.Int64,   # 成交额（元，原始数据是千元）

    # 复权因子 (可选，从 adj_factor 接口获取)
    "adj_factor": pl.Float64,

    # 来源追溯
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}
```

### 1.2 通用行情接口 (pro_bar)

**接口**: `pro_bar` - 整合接口，支持：
- 股票/指数/期货/期权/数字货币
- 日线/周线/月线/分钟线
- 前复权/后复权

**特殊说明**:
- 分钟数据需要 600+ 积分
- 复权机制根据 end_date 动态计算
- 支持均线计算 (ma=[5,20,50])

**建议数据表**:
- `stock_klines_cn` (类似现有 klines 表)
- `index_klines_cn` (指数行情)

### 1.3 每日指标 (daily_basic)

**接口**: `daily_basic` - 每日指标（PE/PB/换手率等）

**输出字段**:
```
ts_code, trade_date, close, turnover_rate, turnover_rate_f,
volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
total_share, float_share, free_share, total_mv, circ_mv
```

**建议数据表**: `stock_daily_metrics_cn`

---

## Phase 2: 财务数据 (Financial Data)

### 2.1 财务指标 (fina_indicator)

**接口**: `fina_indicator` - 财务指标数据

**核心字段分类**:

| 类别 | 主要指标 |
|-----|---------|
| 盈利能力 | eps, roe, roa, grossprofit_margin, netprofit_margin |
| 偿债能力 | current_ratio, quick_ratio, debt_to_assets |
| 营运能力 | assets_turn, inv_turn, ar_turn |
| 成长能力 | *_yoy (同比增长率) |
| 现金流 | ocfps, fcff, fcfe |

**建议数据表**: `stock_financial_indicators_cn`

**Schema 要点**:
```python
STOCK_FINANCIAL_INDICATORS_SCHEMA = {
    # 时间维度
    "ann_date": pl.Date,      # 公告日期
    "end_date": pl.Date,      # 报告期
    "date": pl.Date,          # 分区日期

    # 标识
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "exchange_symbol": pl.Utf8,

    # 盈利能力
    "eps": pl.Float64,
    "dt_eps": pl.Float64,  # 稀释每股收益
    "roe": pl.Float64,
    "roe_dt": pl.Float64,  # 扣非ROE
    "roa": pl.Float64,
    "roic": pl.Float64,
    "grossprofit_margin": pl.Float64,
    "netprofit_margin": pl.Float64,
    "ebitda": pl.Float64,

    # 偿债能力
    "current_ratio": pl.Float64,
    "quick_ratio": pl.Float64,
    "debt_to_assets": pl.Float64,
    "debt_to_eqt": pl.Float64,

    # 营运能力
    "assets_turn": pl.Float64,
    "inv_turn": pl.Float64,
    "ar_turn": pl.Float64,
    "turn_days": pl.Float64,  # 营业周期

    # 成长能力 (同比%)
    "eps_yoy": pl.Float64,
    "roe_yoy": pl.Float64,
    "netprofit_yoy": pl.Float64,
    "dt_netprofit_yoy": pl.Float64,
    "tr_yoy": pl.Float64,  # 营业总收入同比

    # 每股指标
    "bps": pl.Float64,  # 每股净资产
    "ocfps": pl.Float64,
    "cfps": pl.Float64,

    # 来源追溯
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}
```

### 2.2 三大财务报表

| 接口 | 说明 | 建议表名 |
|-----|------|---------|
| `income` | 利润表 | `stock_income_statement_cn` |
| `balance_sheet` | 资产负债表 | `stock_balance_sheet_cn` |
| `cashflow` | 现金流量表 | `stock_cashflow_statement_cn` |

### 2.3 业绩快报 & 预告

| 接口 | 说明 | 使用场景 |
|-----|------|---------|
| `express` | 业绩快报 | 提前获取季度业绩 |
| `forecast` | 业绩预告 | 预期分析 |

---

## Phase 3: 其他重要数据

### 3.1 市场行为数据

| 接口 | 说明 | 建议表名 |
|-----|------|---------|
| `moneyflow` | 个股资金流向 | `stock_moneyflow_cn` |
| `limit_list` | 涨跌停板 | `stock_limit_list_cn` |
| `stk_holdernumber` | 股东户数 | `stock_holder_count_cn` |
| `stk_holdertrade` | 股东增减持 | `stock_holder_trade_cn` |

### 3.2 参考数据

| 接口 | 说明 |
|-----|------|
| `trade_cal` | 交易日历 |
| `concept` | 概念股分类 |
| `industry` | 行业分类 |
| `hk_hold` | 沪深港通持股 |

---

## 数据摄取架构设计

### Bronze 层布局

```
bronze/tushare/
├── type=stock_basic_cn/
│   └── date=2024-01-01/
│       └── snapshot_ts=1704067200000000/
│           └── stock_basic.jsonl.gz
├── type=stock_daily_cn/
│   └── date=2024-01-01/
│       └── exchange=sse/
│           └── daily_20240101.jsonl.gz
├── type=stock_financial_indicators_cn/
│   └── end_date=2023-12-31/
│       └── fina_indicator_2023q4.jsonl.gz
```

### 摄取流程

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Tushare API    │────▶│   Bronze Layer   │────▶│  Silver Layer   │
│                 │     │  (JSONL.GZ)      │     │  (Delta Lake)   │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        │                                               ▲
        │                                               │
        └───────────────────────────────────────────────┘
                         (直接 API 摄取模式 - 可选)
```

### 两种摄取模式

#### 模式 A: API 直接摄取（推荐 for Tushare）

```python
# CLI 命令设计
pointline tushare sync-daily \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --exchange szse \
  --token YOUR_TOKEN

# 内部流程
1. 调用 Tushare API 获取数据
2. 转换为标准 schema
3. 直接写入 Silver (Delta Lake)
4. 记录摄取元数据
```

**优点**:
- 无需 Bronze 存储（节省空间）
- 实时摄取
- 适合 API 类数据源

**缺点**:
- 无法回溯原始 API 响应

#### 模式 B: Bronze → Silver（标准模式）

```python
# 先捕获到 Bronze
pointline tushare capture-daily \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --capture-root ~/data/bronze/tushare

# 再摄取到 Silver
pointline bronze ingest \
  --vendor tushare \
  --data-type stock_daily_cn \
  --glob "type=stock_daily_cn/**/*.jsonl.gz"
```

**优点**:
- 保留原始数据
- 可重新解析
- 符合现有架构

---

## 实施优先级

### Phase 1: 核心行情数据 (Week 1-2)

1. **stock_daily_cn** - 日线行情
   - 最高优先级，最常用
   - 支持 SZSE/SSE 全量股票

2. **daily_basic_cn** - 每日指标
   - PE/PB/市值等关键指标
   - 与日线行情联合使用

### Phase 2: 财务数据 (Week 3-4)

1. **stock_financial_indicators_cn** - 财务指标
   - 季度数据
   - PIT 正确性处理（公告日期 vs 报告期）

2. **三大报表**（按需实现）

### Phase 3: 扩展数据 (Week 5+)

1. **资金流向** - moneyflow
2. **涨跌停** - limit_list
3. **分钟线** - pro_bar (分钟)

---

## 关键技术考虑

### PIT (Point-in-Time) 正确性

财务数据需要特殊处理：

```python
# 问题：2024-03-31 的财报可能在 2024-04-30 才公告
# 研究时应该使用公告日期作为有效时间

# 解决方案
- 使用 ann_date (公告日期) 作为 valid_from
- 使用 end_date (报告期) 作为数据标识
- 研究时按公告日期过滤，避免前视偏差
```

### 复权处理

```python
# 方案 1: 存储复权因子，查询时计算
# 方案 2: 存储多条记录（未复权/前复权/后复权）

# 推荐方案 1（节省空间，灵活）
df.with_columns([
    (pl.col("close_px_int") * pl.col("adj_factor")).alias("close_adj_qfq")
])
```

### 数据完整性检查

```python
# 每日检查项
def validate_daily_data(df: pl.DataFrame) -> pl.DataFrame:
    checks = [
        # 价格逻辑
        (pl.col("high_px_int") >= pl.col("low_px_int")),
        (pl.col("high_px_int") >= pl.col("open_px_int")),
        (pl.col("high_px_int") >= pl.col("close_px_int")),

        # 成交量额
        (pl.col("volume_qty_int") >= 0),
        (pl.col("amount_quote_int") >= 0),

        # 涨跌幅限制（考虑科创板/创业板 20%）
        (pl.col("pct_chg").abs() <= 20.0),
    ]
    ...
```

---

## CLI 命令设计

```bash
# 数据同步
pointline tushare sync-daily --start-date 2024-01-01 --end-date 2024-12-31
pointline tushare sync-financial --quarters 2023Q4,2024Q1,2024Q2
pointline tushare sync-metrics --date 2024-01-15

# 数据查询
pointline tushare query-daily --symbol 000001.SZ --start-date 2024-01-01
pointline tushare query-financial --symbol 000001.SZ --indicator roe,eps

# 数据质量
pointline tushare validate-daily --date 2024-01-15
pointline tushare validate-financial --quarter 2023Q4

# 全量同步（初始化）
pointline tushare sync-all --start-date 2020-01-01 --exchange all
```

---

## 与研究框架集成

### 特征工程示例

```python
from pointline import research
import polars as pl

# 加载日线数据
daily = research.query(
    table="stock_daily_cn",
    exchange="szse",
    symbols=["000001.SZ"],
    start_date="2024-01-01",
    end_date="2024-12-31"
)

# 计算技术指标
daily = daily.with_columns([
    # 收益率
    (pl.col("close_px_int") / pl.col("close_px_int").shift(1) - 1).alias("return_1d"),

    # 20日均线
    pl.col("close_px_int").rolling_mean(20).alias("ma20"),

    # 波动率
    pl.col("return_1d").rolling_std(20).alias("volatility_20d"),

    # 成交量均线
    pl.col("volume_qty_int").rolling_mean(20).alias("volume_ma20"),
])

# 联合财务数据
financial = research.query(
    table="stock_financial_indicators_cn",
    exchange="szse",
    symbols=["000001.SZ"],
)

# PIT join (as-of join)
combined = daily.join_asof(
    financial,
    left_on="date",
    right_on="ann_date",
    strategy="backward"
)
```

---

## 下一步行动

1. **确认优先级**：哪些数据表对研究团队最紧急？
2. **积分检查**：确认 Tushare 账户积分是否足够（建议 5000+）
3. **Schema 评审**：评审上述 schema 设计是否满足需求
4. **实施 Phase 1**：从 stock_daily_cn 开始实现

---

## 参考链接

- [Tushare Pro 文档](https://tushare.pro/document/2)
- [日线行情接口](https://tushare.pro/document/2?doc_id=27)
- [财务指标接口](https://tushare.pro/document/2?doc_id=79)
- [通用行情接口](https://tushare.pro/document/2?doc_id=109)
