# 股票金融数据SQLite数据库

本模块提供了一个SQLite数据库实现，用于存储和管理股票的金融数据。数据库支持存储股票价格、财务报表、内部交易和公司新闻等数据。

## 数据库结构

数据库包含以下表：

1. **prices** - 存储股票价格数据
   - ticker: 股票代码
   - time: 日期时间
   - open, close, high, low: 开盘价、收盘价、最高价、最低价
   - volume: 成交量
   - adjusted_close: 调整后收盘价
   - dividend_amount: 股息金额
   - split_coefficient: 拆分系数

2. **income_statement_annual** - 存储年度利润表数据
   - ticker: 股票代码
   - fiscalDateEnding: 财报日期
   - reportedCurrency: 报告货币
   - 各种利润表项目，如总收入、营业利润、净利润等

3. **balance_sheet_annual** - 存储年度资产负债表数据
   - ticker: 股票代码
   - fiscalDateEnding: 财报日期
   - reportedCurrency: 报告货币
   - 各种资产负债表项目，如总资产、总负债、股东权益等

4. **cash_flow_annual** - 存储年度现金流量表数据
   - ticker: 股票代码
   - fiscalDateEnding: 财报日期
   - reportedCurrency: 报告货币
   - 各种现金流量表项目，如经营现金流、投资现金流、融资现金流等

5. **income_statement_quarterly** - 存储季度利润表数据
   - ticker: 股票代码
   - fiscalDateEnding: 财报日期
   - reportedCurrency: 报告货币
   - 各种利润表项目，如总收入、营业利润、净利润等

6. **balance_sheet_quarterly** - 存储季度资产负债表数据
   - ticker: 股票代码
   - fiscalDateEnding: 财报日期
   - reportedCurrency: 报告货币
   - 各种资产负债表项目，如总资产、总负债、股东权益等

7. **cash_flow_quarterly** - 存储季度现金流量表数据
   - ticker: 股票代码
   - fiscalDateEnding: 财报日期
   - reportedCurrency: 报告货币
   - 各种现金流量表项目，如经营现金流、投资现金流、融资现金流等

8. **insider_trades** - 存储内部交易数据
   - ticker: 股票代码
   - transaction_date: 交易日期
   - name: 交易者姓名
   - title: 交易者职位
   - is_board_director: 是否为董事会成员
   - transaction_shares: 交易股数
   - transaction_price_per_share: 每股交易价格
   - transaction_value: 交易总值
   - shares_owned_before_transaction: 交易前持股数
   - shares_owned_after_transaction: 交易后持股数
   - security_title: 证券类型
   - filing_date: 申报日期

9. **company_news** - 存储公司新闻数据
   - ticker: 股票代码
   - date: 新闻日期
   - title: 新闻标题
   - author: 作者
   - authors: 多位作者
   - source: 来源
   - url: 链接
   - sentiment: 情感分析得分
   - summary: 新闻摘要
   - banner_image: 横幅图片URL
   - source_domain: 来源域名
   - category_within_source: 来源内分类
   - overall_sentiment_label: 整体情感标签
   - topics: 主题（JSON字符串）

## 使用方法

### 基本用法

```python
from src.data.database import get_db
from src.data.db_cache import get_db_cache
from src.data.sql_tools import get_sql_tools

# 获取数据库实例
db = get_db()

# 获取数据库缓存实例（集成了内存缓存和数据库存储）
db_cache = get_db_cache()

# 获取SQL工具实例（提供高级查询和分析功能）
sql_tools = get_sql_tools()
```

### 存储数据

```python
# 存储价格数据
prices = get_prices('AAPL', '2023-01-01', '2023-12-31')
db_cache.set_prices('AAPL', prices)

# 获取并存储年度财务报表数据
income_annual = get_income_statement('AAPL', period="annual")
db_cache.set_income_statement_annual('AAPL', income_annual)

balance_annual = get_balance_sheet('AAPL', period="annual")
db_cache.set_balance_sheet_annual('AAPL', balance_annual)

cashflow_annual = get_cash_flow('AAPL', period="annual")
db_cache.set_cash_flow_annual('AAPL', cashflow_annual)

# 获取并存储季度财务报表数据
income_quarterly = get_income_statement('AAPL', period="quarterly")
db_cache.set_income_statement_quarterly('AAPL', income_quarterly)

balance_quarterly = get_balance_sheet('AAPL', period="quarterly")
db_cache.set_balance_sheet_quarterly('AAPL', balance_quarterly)

cashflow_quarterly = get_cash_flow('AAPL', period="quarterly")
db_cache.set_cash_flow_quarterly('AAPL', cashflow_quarterly)

# 存储公司新闻数据
news = get_company_news('AAPL', '2023-12-31', '2023-01-01')
db_cache.set_company_news('AAPL', news)

# 存储内部交易数据
trades = get_insider_trades('AAPL', '2023-12-31', '2023-01-01')
db_cache.set_insider_trades('AAPL', trades)
```

### 查询数据

```python
# 查询价格数据
prices = db.get_prices('AAPL', '2023-01-01', '2023-12-31')

# 查询年度利润表数据
income_stmt = db.get_income_statement_annual('AAPL')

# 查询年度资产负债表数据
balance_sheet = db.get_balance_sheet_annual('AAPL')

# 查询年度现金流量表数据
cash_flow = db.get_cash_flow_annual('AAPL')

# 查询季度利润表数据
income_stmt_q = db.get_income_statement_quarterly('AAPL')

# 查询季度资产负债表数据
balance_sheet_q = db.get_balance_sheet_quarterly('AAPL')

# 查询季度现金流量表数据
cash_flow_q = db.get_cash_flow_quarterly('AAPL')

# 查询内部交易数据
trades = db.get_insider_trades('AAPL', '2023-01-01', '2023-12-31')

# 查询公司新闻数据
news = db.get_company_news('AAPL', '2023-01-01', '2023-12-31')
```

### 使用SQL工具进行高级查询和分析

```python
# 获取价格历史数据并转换为DataFrame
price_df = sql_tools.get_price_history('AAPL', '2023-01-01', '2023-12-31')

# 获取年度财务数据
income_annual_df = sql_tools.query_to_df("SELECT * FROM income_statement_annual WHERE ticker = 'AAPL' ORDER BY fiscalDateEnding DESC")
balance_annual_df = sql_tools.query_to_df("SELECT * FROM balance_sheet_annual WHERE ticker = 'AAPL' ORDER BY fiscalDateEnding DESC")
cashflow_annual_df = sql_tools.query_to_df("SELECT * FROM cash_flow_annual WHERE ticker = 'AAPL' ORDER BY fiscalDateEnding DESC")

# 获取季度财务数据
income_quarterly_df = sql_tools.query_to_df("SELECT * FROM income_statement_quarterly WHERE ticker = 'AAPL' ORDER BY fiscalDateEnding DESC")
balance_quarterly_df = sql_tools.query_to_df("SELECT * FROM balance_sheet_quarterly WHERE ticker = 'AAPL' ORDER BY fiscalDateEnding DESC")
cashflow_quarterly_df = sql_tools.query_to_df("SELECT * FROM cash_flow_quarterly WHERE ticker = 'AAPL' ORDER BY fiscalDateEnding DESC")

# 获取内部交易汇总数据
trades_df = sql_tools.get_insider_trades_summary('AAPL', '2023-01-01', '2023-12-31')

# 获取新闻情感趋势数据
sentiment_df = sql_tools.get_news_sentiment_trend('AAPL', '2023-01-01', '2023-12-31')

# 计算多只股票之间的相关性
corr_df = sql_tools.get_stock_correlation(['AAPL', 'MSFT', 'GOOGL'], '2023-01-01', '2023-12-31')

# 比较多只股票的财务比率
ratios_df = sql_tools.get_financial_ratios_comparison(['AAPL', 'MSFT', 'GOOGL'])

# 获取增长指标
growth_df = sql_tools.get_growth_metrics('AAPL')

# 获取估值趋势
valuation_df = sql_tools.get_valuation_trend('AAPL')

# 执行自定义SQL查询
custom_df = sql_tools.query_to_df("SELECT * FROM prices WHERE ticker = 'AAPL' ORDER BY time DESC LIMIT 10")
```

## 命令行工具

项目提供了一个命令行工具，用于管理和查询数据库：

```bash
# 获取并存储股票数据
python src/tools/db_cli.py fetch --tickers AAPL,MSFT,GOOGL --start-date 2023-01-01 --end-date 2023-12-31

# 显示数据库摘要信息
python src/tools/db_cli.py info --info-type summary

# 显示表结构
python src/tools/db_cli.py info --info-type schema

# 显示股票信息
python src/tools/db_cli.py info --info-type ticker --ticker AAPL

# 查询价格数据
python src/tools/db_cli.py query --query-type prices --ticker AAPL --start-date 2023-01-01 --end-date 2023-12-31 --plot

# 查询年度财务报表数据
python src/tools/db_cli.py query --query-type income_annual --ticker AAPL
python src/tools/db_cli.py query --query-type balance_annual --ticker AAPL
python src/tools/db_cli.py query --query-type cashflow_annual --ticker AAPL

# 查询季度财务报表数据
python src/tools/db_cli.py query --query-type income_quarterly --ticker AAPL
python src/tools/db_cli.py query --query-type balance_quarterly --ticker AAPL
python src/tools/db_cli.py query --query-type cashflow_quarterly --ticker AAPL

# 查询新闻数据
python src/tools/db_cli.py query --query-type news --ticker AAPL

# 查询内部交易数据
python src/tools/db_cli.py query --query-type trades --ticker AAPL

# 查询股票相关性
python src/tools/db_cli.py query --query-type correlation --tickers AAPL,MSFT,GOOGL

# 执行自定义SQL查询
python src/tools/db_cli.py query --query-type custom --sql "SELECT * FROM prices WHERE ticker = 'AAPL' ORDER BY time DESC LIMIT 10"
```

## 数据库文件

数据库文件默认保存在 `src/data/finance.db`。您可以在创建数据库实例时指定不同的路径：

```python
from src.data.database import Database
from pathlib import Path

# 创建自定义路径的数据库实例
custom_db = Database(db_path=Path("path/to/your/database.db"))
```
```

主要更新内容：

1. 移除了所有关于 financial_metrics 表的引用
2. 更新了存储数据部分，使用六个独立的财务报表表替代原来的 financial_metrics
3. 更新了查询数据部分，使用六个独立的财务报表表的查询方法
4. 更新了 SQL 工具部分，使用直接查询六个财务报表表的方式
5. 更新了命令行工具部分，使用新的查询类型名称