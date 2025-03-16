#!/usr/bin/env python
"""
SQLite数据库命令行工具，用于管理和查询股票金融数据
"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib
from tabulate import tabulate

# 设置matplotlib使用支持中文的字体
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei', 'DejaVu Sans']
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 添加项目根目录到路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data.database import get_db
from src.data.db_cache import get_db_cache
from src.data.sql_tools import get_sql_tools
from src.tools.api import get_prices, get_financial_metrics, get_company_news, get_insider_trades

def fetch_data(args):
    """获取并存储股票数据"""
    db_cache = get_db_cache()
    
    # 解析日期参数
    if args.end_date:
        end_date = args.end_date
    else:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    if args.start_date:
        start_date = args.start_date
    else:
        # 对于价格数据，默认获取所有历史数据
        if args.data_type in ['all', 'prices']:
            start_date = "full"
        else:
            # 对于其他类型的数据，默认获取一年的数据
            start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')
    
    # 获取股票列表
    tickers = args.tickers.split(',')
    
    print(f"正在获取并存储 {', '.join(tickers)} 的数据...")
    
    # 获取并存储股票数据
    for ticker in tickers:
        # 获取价格数据
        if args.data_type in ['all', 'prices']:
            print(f"获取 {ticker} 的价格数据...")
            
            # 检查当前是否为周末，且请求的是最新数据
            current_date = datetime.now().date()
            is_weekend = current_date.weekday() >= 5  # 5是周六，6是周日
            
            # 获取数据库中最新的日期
            db = get_db()
            db_data = db.get_prices(ticker)
            if db_data and len(db_data) > 0:
                db_latest_date = max(item['time'] for item in db_data)
                db_latest_datetime = datetime.strptime(db_latest_date, '%Y-%m-%d').date()
                is_db_friday = db_latest_datetime.weekday() == 4  # 4是周五
                
                # 如果当前是周末，且数据库最新日期是周五，且相差不超过2天，则不需要更新
                if is_weekend and is_db_friday and (current_date - db_latest_datetime).days <= 2:
                    print(f"当前是周末（{current_date}），数据库最新日期为周五（{db_latest_date}），无需更新")
                    # 直接从数据库获取数据
                    if start_date == "full":
                        # 如果start_date是"full"，则获取所有历史数据
                        prices = db_data
                    else:
                        # 否则根据日期范围过滤
                        prices = [item for item in db_data if item['time'] >= start_date and (not end_date or item['time'] <= end_date)]
                    print(f"从数据库获取 {len(prices)} 条价格记录")
                else:
                    # 正常获取数据
                    prices = get_prices(ticker, start_date, end_date)
                    db_cache.set_prices(ticker, prices)
                    print(f"已存储 {len(prices)} 条价格记录")
            else:
                # 数据库中没有数据，正常获取
                prices = get_prices(ticker, start_date, end_date)
                db_cache.set_prices(ticker, prices)
                print(f"已存储 {len(prices)} 条价格记录")
        
        # 获取财务指标数据
        if args.data_type in ['all', 'metrics']:
            print(f"获取 {ticker} 的财务指标数据...")
            metrics = get_financial_metrics(ticker)
            db_cache.set_financial_metrics(ticker, metrics)
            print(f"已存储 {len(metrics)} 条财务指标记录")
        
        # 获取公司新闻数据
        if args.data_type in ['all', 'news']:
            print(f"获取 {ticker} 的公司新闻数据...")
            news = get_company_news(ticker, end_date, start_date)
            db_cache.set_company_news(ticker, news)
            print(f"已存储 {len(news)} 条新闻记录")
        
        # 获取内部交易数据
        if args.data_type in ['all', 'trades']:
            print(f"获取 {ticker} 的内部交易数据...")
            trades = get_insider_trades(ticker, end_date, start_date)
            db_cache.set_insider_trades(ticker, trades)
            print(f"已存储 {len(trades)} 条内部交易记录")
    
    print("数据获取和存储完成!")

def show_info(args):
    """显示数据库信息"""
    db = get_db()
    sql_tools = get_sql_tools()
    
    if args.info_type == 'summary':
        # 显示数据库摘要信息
        summary = sql_tools.get_database_summary()
        print("\n数据库摘要信息:")
        print(f"数据库大小: {summary['db_size'] / 1024 / 1024:.2f} MB")
        print(f"股票数量: {summary['ticker_count']}")
        print(f"股票列表: {', '.join(summary['tickers'])}")
        
        for table_name, table_info in summary['tables'].items():
            print(f"\n表 {table_name} 的记录数: {table_info['count']}")
    
    elif args.info_type == 'schema':
        # 显示表结构
        tables = db.get_tables()
        for table_name in tables:
            print(f"\n表 {table_name} 的结构:")
            schema = db.get_table_schema(table_name)
            schema_df = pd.DataFrame([dict(row) for row in schema])
            print(tabulate(schema_df, headers='keys', tablefmt='psql'))
    
    elif args.info_type == 'ticker':
        # 显示股票信息
        if not args.ticker:
            print("错误: 需要指定股票代码 (--ticker)")
            return
        
        stats = db.get_ticker_stats(args.ticker)
        print(f"\n股票 {args.ticker} 的统计信息:")
        for table, count in stats.items():
            if table == 'price_date_range':
                print(f"价格数据日期范围: {stats['price_date_range']['start']} 至 {stats['price_date_range']['end']}")
            else:
                print(f"{table} 表中的记录数: {count}")

def query_data(args):
    """查询数据"""
    sql_tools = get_sql_tools()
    
    if args.query_type == 'prices':
        # 查询价格数据
        if not args.ticker:
            print("错误: 需要指定股票代码 (--ticker)")
            return
        
        df = sql_tools.get_price_history(args.ticker, args.start_date, args.end_date)
        if df.empty:
            print(f"没有找到 {args.ticker} 的价格数据")
            return
        
        print(f"\n{args.ticker} 的价格数据 ({len(df)} 条记录):")
        print(tabulate(df.head(10), headers='keys', tablefmt='psql'))
        
        if args.plot:
            plt.figure(figsize=(12, 6))
            plt.plot(df.index, df['close'])
            plt.title(f'{args.ticker} 股价走势')
            plt.xlabel('日期')
            plt.ylabel('价格')
            plt.grid(True)
            
            if not args.no_save:
                plt.savefig(f'{args.ticker}_price.png')
                print(f"价格图表已保存为 {args.ticker}_price.png")
            
            plt.show()  # 显示图表
    
    elif args.query_type == 'metrics':
        # 查询财务指标数据
        if not args.ticker:
            print("错误: 需要指定股票代码 (--ticker)")
            return
        
        df = sql_tools.get_financial_metrics_history(args.ticker)
        if df.empty:
            print(f"没有找到 {args.ticker} 的财务指标数据")
            return
        
        print(f"\n{args.ticker} 的财务指标数据 ({len(df)} 条记录):")
        print(tabulate(df.head(10), headers='keys', tablefmt='psql'))
    
    elif args.query_type == 'news':
        # 查询新闻数据
        if not args.ticker:
            print("错误: 需要指定股票代码 (--ticker)")
            return
        
        sql = "SELECT * FROM company_news WHERE ticker = ?"
        params = [args.ticker]
        
        if args.start_date:
            sql += " AND date >= ?"
            params.append(args.start_date)
        
        if args.end_date:
            sql += " AND date <= ?"
            params.append(args.end_date)
        
        sql += " ORDER BY date DESC LIMIT 20"
        
        df = sql_tools.query_to_df(sql, params)
        if df.empty:
            print(f"没有找到 {args.ticker} 的新闻数据")
            return
        
        print(f"\n{args.ticker} 的新闻数据 ({len(df)} 条记录):")
        print(tabulate(df[['date', 'title', 'sentiment']], headers='keys', tablefmt='psql'))
    
    elif args.query_type == 'trades':
        # 查询内部交易数据
        if not args.ticker:
            print("错误: 需要指定股票代码 (--ticker)")
            return
        
        df = sql_tools.get_insider_trades_summary(args.ticker, args.start_date, args.end_date)
        if df.empty:
            print(f"没有找到 {args.ticker} 的内部交易数据")
            return
        
        print(f"\n{args.ticker} 的内部交易汇总数据 ({len(df)} 条记录):")
        print(tabulate(df.head(10), headers='keys', tablefmt='psql'))
    
    elif args.query_type == 'correlation':
        # 查询股票相关性
        if not args.tickers:
            print("错误: 需要指定股票代码列表 (--tickers)")
            return
        
        tickers = args.tickers.split(',')
        df = sql_tools.get_stock_correlation(tickers, args.start_date, args.end_date)
        if df.empty:
            print(f"没有找到足够的数据计算相关性")
            return
        
        print(f"\n股票相关性矩阵:")
        print(tabulate(df, headers='keys', tablefmt='psql'))
    
    elif args.query_type == 'custom':
        # 执行自定义SQL查询
        if not args.sql:
            print("错误: 需要指定SQL查询语句 (--sql)")
            return
        
        df = sql_tools.query_to_df(args.sql)
        if df.empty:
            print("查询结果为空")
            return
        
        print("\n查询结果:")
        print(tabulate(df.head(20), headers='keys', tablefmt='psql'))
        print(f"总记录数: {len(df)}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='SQLite数据库命令行工具')
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # fetch命令
    fetch_parser = subparsers.add_parser('fetch', help='获取并存储股票数据')
    fetch_parser.add_argument('--tickers', required=True, help='股票代码列表，用逗号分隔')
    fetch_parser.add_argument('--start-date', help='起始日期 (YYYY-MM-DD 或 "full" 表示获取所有历史数据)')
    fetch_parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)')
    fetch_parser.add_argument('--data-type', choices=['all', 'prices', 'metrics', 'news', 'trades'], default='all', help='数据类型')
    
    # info命令
    info_parser = subparsers.add_parser('info', help='显示数据库信息')
    info_parser.add_argument('--info-type', choices=['summary', 'schema', 'ticker'], default='summary', help='信息类型')
    info_parser.add_argument('--ticker', help='股票代码 (用于ticker信息类型)')
    
    # query命令
    query_parser = subparsers.add_parser('query', help='查询数据')
    query_parser.add_argument('--query-type', choices=['prices', 'metrics', 'news', 'trades', 'correlation', 'custom'], required=True, help='查询类型')
    query_parser.add_argument('--ticker', help='股票代码')
    query_parser.add_argument('--tickers', help='股票代码列表，用逗号分隔 (用于correlation查询类型)')
    query_parser.add_argument('--start-date', help='起始日期 (YYYY-MM-DD)')
    query_parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)')
    query_parser.add_argument('--sql', help='自定义SQL查询语句 (用于custom查询类型)')
    query_parser.add_argument('--plot', action='store_true', help='绘制图表 (用于prices查询类型)')
    query_parser.add_argument('--no-save', action='store_true', help='不保存图表文件，仅显示 (用于--plot)')
    
    args = parser.parse_args()
    
    if args.command == 'fetch':
        fetch_data(args)
    elif args.command == 'info':
        show_info(args)
    elif args.command == 'query':
        query_data(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
