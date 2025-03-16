"""
SQLite数据库示例脚本，演示如何使用数据库存储和查询股票数据
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data.db_cache import get_db_cache
from src.data.sql_tools import get_sql_tools
from src.tools.api import get_prices, get_financial_metrics, get_company_news, get_insider_trades

def main():
    """主函数"""
    # 获取数据库缓存实例
    db_cache = get_db_cache()
    
    # 获取SQL工具实例
    sql_tools = get_sql_tools()
    
    # 设置示例股票和日期范围
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = yesterday
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')  # 查询最近一周的数据
    
    print(f"正在获取并存储 {', '.join(tickers)} 的数据...")
    
    # 获取并存储股票数据
    for ticker in tickers:
        # 获取价格数据
        print(f"获取 {ticker} 的价格数据...")
        prices = get_prices(ticker, start_date, end_date)
        db_cache.set_prices(ticker, prices)
        
        # 获取财务指标数据
        print(f"获取 {ticker} 的财务指标数据...")
        metrics = get_financial_metrics(ticker)
        db_cache.set_financial_metrics(ticker, metrics)
        
        # 获取公司新闻数据
        print(f"获取 {ticker} 的公司新闻数据...")
        news = get_company_news(ticker, end_date, start_date)
        db_cache.set_company_news(ticker, news)
        
        # 获取内部交易数据
        print(f"获取 {ticker} 的内部交易数据...")
        trades = get_insider_trades(ticker, end_date, start_date)
        db_cache.set_insider_trades(ticker, trades)
    
    # 显示数据库摘要信息
    print("\n数据库摘要信息:")
    summary = sql_tools.get_database_summary()
    print(f"数据库大小: {summary['db_size'] / 1024 / 1024:.2f} MB")
    print(f"股票数量: {summary['ticker_count']}")
    print(f"股票列表: {', '.join(summary['tickers'])}")
    
    for table_name, table_info in summary['tables'].items():
        print(f"\n表 {table_name} 的记录数: {table_info['count']}")
    
    # 示例查询: 获取股票价格历史
    print("\n示例查询: 获取AAPL的价格历史")
    price_df = sql_tools.get_price_history('AAPL', start_date, end_date)
    if not price_df.empty:
        print(f"获取到 {len(price_df)} 条价格记录")
        print(price_df.head())
        
        # 绘制价格图表
        plt.figure(figsize=(12, 6))
        plt.plot(price_df.index, price_df['close'])
        plt.title('AAPL Stock Price Trend')
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.grid(True)
        plt.savefig('aapl_price.png')
        print("价格图表已保存为 aapl_price.png")
    
    # 示例查询: 获取财务指标比较
    print("\n示例查询: 比较多只股票的财务指标")
    ratios_df = sql_tools.get_financial_ratios_comparison(tickers)
    if not ratios_df.empty:
        print(ratios_df)
    
    # 示例查询: 获取股票相关性
    print("\n示例查询: 计算股票相关性")
    corr_df = sql_tools.get_stock_correlation(tickers, start_date, end_date)
    if not corr_df.empty:
        print(corr_df)
    
    # 示例查询: 获取新闻情感趋势
    print("\n示例查询: 获取AAPL的新闻情感趋势")
    # 先检查数据库中是否有新闻数据
    news_check_sql = "SELECT COUNT(*) as count, COUNT(sentiment) as sentiment_count FROM company_news WHERE ticker = 'AAPL'"
    news_check_df = sql_tools.query_to_df(news_check_sql)
    print(f"AAPL新闻数据统计: {news_check_df.iloc[0].to_dict()}")
    
    # 检查日期格式
    date_check_sql = "SELECT date, sentiment FROM company_news WHERE ticker = 'AAPL' LIMIT 5"
    date_check_df = sql_tools.query_to_df(date_check_sql)
    if not date_check_df.empty:
        print("AAPL新闻日期和情感样本:")
        print(date_check_df)
    
    # 打印日期范围
    print(f"查询日期范围: start_date={start_date}, end_date={end_date}")
    
    # 检查日期范围内的记录
    date_range_sql = f"SELECT COUNT(*) as count FROM company_news WHERE ticker = 'AAPL' AND date >= '{start_date}' AND date <= '{end_date}'"
    date_range_df = sql_tools.query_to_df(date_range_sql)
    print(f"日期范围内的记录数: {date_range_df.iloc[0]['count']}")
    
    sentiment_df = sql_tools.get_news_sentiment_trend('AAPL', start_date, end_date)
    if not sentiment_df.empty:
        print(f"获取到 {len(sentiment_df)} 条情感记录")
        print(sentiment_df.head())
    else:
        print("情感趋势查询结果为空")
        # 检查SQL查询
        debug_sql = """
        SELECT 
            date,
            AVG(sentiment) as avg_sentiment,
            COUNT(*) as news_count
        FROM company_news
        WHERE ticker = 'AAPL' AND sentiment IS NOT NULL
        GROUP BY date ORDER BY date
        """
        debug_df = sql_tools.query_to_df(debug_sql)
        print(f"直接SQL查询结果: {len(debug_df)} 条记录")
        if not debug_df.empty:
            print(debug_df.head())
    
    # 示例查询: 获取内部交易汇总
    print("\n示例查询: 获取AAPL的内部交易汇总")
    trades_df = sql_tools.get_insider_trades_summary('AAPL', start_date, end_date)
    if not trades_df.empty:
        print(f"获取到 {len(trades_df)} 条内部交易记录")
        print(trades_df.head())
    
    # 示例查询: 执行自定义SQL查询
    print("\n示例查询: 执行自定义SQL查询")
    custom_sql = """
    SELECT 
        p.ticker, 
        COUNT(p.id) as price_count,
        MIN(p.time) as earliest_date,
        MAX(p.time) as latest_date,
        AVG(p.close) as avg_price,
        (SELECT COUNT(*) FROM company_news n WHERE n.ticker = p.ticker) as news_count
    FROM prices p
    GROUP BY p.ticker
    """
    custom_df = sql_tools.query_to_df(custom_sql)
    if not custom_df.empty:
        print(custom_df)
    
    print("\n数据库示例完成!")

if __name__ == "__main__":
    main()
