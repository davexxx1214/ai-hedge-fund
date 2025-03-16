# 股票金融数据SQLite数据库

本模块提供了一个SQLite数据库实现，用于存储和管理股票的金融数据。数据库支持存储股票价格、财务指标、财务项目、内部交易和公司新闻等数据。

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

2. **financial_metrics** - 存储财务指标数据
   - ticker: 股票代码
   - report_period: 报告期
   - 各种财务比率和指标，如市盈率、市净率、毛利率等

3. **line_items** - 存储财务项目数据
   - ticker: 股票代码
   - report_period: 报告期
   - item_name: 项目名称
   - item_value: 项目值

4. **insider_trades** - 存储内部交易数据
   - ticker: 股票代码
   - transaction_date: 交易日期
   - name: 交易者姓名
   - title: 交易者职位
   - transaction_shares: 交易股数
   - transaction_price_per_share: 每股交易价格
   - transaction_value: 交易总值

5. **company_news** - 存储公司新闻数据
   - ticker: 股票代码
   - date: 新闻日期
   - title: 新闻标题
   - author: 作者
   - source: 来源
   - url: 链接
   - sentiment: 情感分析得分

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

# 存储财务指标数据
metrics = get_financial_metrics('AAPL')
db_cache.set_financial_metrics('AAPL', metrics)

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

# 查询财务指标数据
metrics = db.get_financial_metrics('AAPL')

# 查询财务项目数据
items = db.get_line_items('AAPL', item_names=['revenue', 'net_income'])

# 查询内部交易数据
trades = db.get_insider_trades('AAPL', '2023-01-01', '2023-12-31')

# 查询公司新闻数据
news = db.get_company_news('AAPL', '2023-01-01', '2023-12-31')
```

### 使用SQL工具进行高级查询和分析

```python
# 获取价格历史数据并转换为DataFrame
price_df = sql_tools.get_price_history('AAPL', '2023-01-01', '2023-12-31')

# 获取财务指标历史数据
metrics_df = sql_tools.get_financial_metrics_history('AAPL')

# 获取财务项目数据并透视为时间序列
items_df = sql_tools.get_line_items_pivot('AAPL', ['revenue', 'net_income'])

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

# 查询财务指标数据
python src/tools/db_cli.py query --query-type metrics --ticker AAPL

# 查询新闻数据
python src/tools/db_cli.py query --query-type news --ticker AAPL

# 查询内部交易数据
python src/tools/db_cli.py query --query-type trades --ticker AAPL

# 查询股票相关性
python src/tools/db_cli.py query --query-type correlation --tickers AAPL,MSFT,GOOGL

# 执行自定义SQL查询
python src/tools/db_cli.py query --query-type custom --sql "SELECT * FROM prices WHERE ticker = 'AAPL' ORDER BY time DESC LIMIT 10"
```

## 示例脚本

项目提供了一个示例脚本，演示如何使用数据库存储和查询股票数据：

```bash
python src/examples/db_example.py
```

该脚本会获取并存储几只股票的数据，然后执行一些示例查询，并生成价格图表。

## 数据库文件

数据库文件默认保存在 `src/data/finance.db`。您可以在创建数据库实例时指定不同的路径：

```python
from src.data.database import Database

# 创建自定义路径的数据库实例
custom_db = Database(db_path=Path("path/to/your/database.db"))
