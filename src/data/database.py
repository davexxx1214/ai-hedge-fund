import os
import sqlite3
import json
import threading
from datetime import datetime
from pathlib import Path
import pandas as pd

# 数据库文件路径
DB_PATH = Path("src/data/finance.db")

class Database:
    """SQLite数据库管理类，用于存储股票金融数据"""

    def __init__(self, db_path=DB_PATH):
        """初始化数据库连接并创建表结构"""
        self.db_path = db_path
        # 确保数据库目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 创建数据库连接
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row  # 使查询结果可以通过列名访问
        
        # 创建表结构
        self._create_tables()
    
    def _create_tables(self):
        """创建数据库表结构"""
        cursor = self.conn.cursor()
        
        # 创建股票价格表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            time TEXT NOT NULL,
            open REAL,
            close REAL,
            high REAL,
            low REAL,
            volume INTEGER,
            adjusted_close REAL,
            dividend_amount REAL,
            split_coefficient REAL,
            UNIQUE(ticker, time)
        )
        ''')
        
        # 创建财务指标表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            report_period TEXT NOT NULL,
            period TEXT,
            currency TEXT,
            market_cap REAL,
            enterprise_value REAL,
            price_to_earnings_ratio REAL,
            price_to_book_ratio REAL,
            price_to_sales_ratio REAL,
            enterprise_value_to_ebitda_ratio REAL,
            enterprise_value_to_revenue_ratio REAL,
            free_cash_flow_yield REAL,
            peg_ratio REAL,
            gross_margin REAL,
            operating_margin REAL,
            net_margin REAL,
            return_on_equity REAL,
            return_on_assets REAL,
            return_on_invested_capital REAL,
            asset_turnover REAL,
            inventory_turnover REAL,
            receivables_turnover REAL,
            days_sales_outstanding REAL,
            operating_cycle REAL,
            working_capital_turnover REAL,
            current_ratio REAL,
            quick_ratio REAL,
            cash_ratio REAL,
            operating_cash_flow_ratio REAL,
            debt_to_equity REAL,
            debt_to_assets REAL,
            interest_coverage REAL,
            revenue_growth REAL,
            earnings_growth REAL,
            book_value_growth REAL,
            earnings_per_share_growth REAL,
            free_cash_flow_growth REAL,
            operating_income_growth REAL,
            ebitda_growth REAL,
            payout_ratio REAL,
            earnings_per_share REAL,
            book_value_per_share REAL,
            free_cash_flow_per_share REAL,
            UNIQUE(ticker, report_period)
        )
        ''')
        
        # 创建财务项目表 (动态字段)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS line_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            report_period TEXT NOT NULL,
            period TEXT,
            currency TEXT,
            item_name TEXT NOT NULL,
            item_value REAL,
            UNIQUE(ticker, report_period, item_name)
        )
        ''')
        
        # 创建内部交易表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS insider_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            issuer TEXT,
            name TEXT,
            title TEXT,
            is_board_director INTEGER,
            transaction_date TEXT NOT NULL,
            transaction_shares REAL,
            transaction_price_per_share REAL,
            transaction_value REAL,
            shares_owned_before_transaction REAL,
            shares_owned_after_transaction REAL,
            security_title TEXT,
            filing_date TEXT NOT NULL,
            UNIQUE(ticker, name, transaction_date, filing_date)
        )
        ''')
        
        # 创建公司新闻表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS company_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            title TEXT NOT NULL,
            author TEXT,
            authors TEXT,  -- 添加authors字段
            source TEXT,
            date TEXT NOT NULL,
            url TEXT,
            sentiment REAL,
            summary TEXT,  -- 添加summary字段
            banner_image TEXT,  -- 添加banner_image字段
            source_domain TEXT,  -- 添加source_domain字段
            category_within_source TEXT,  -- 添加category_within_source字段
            overall_sentiment_label TEXT,  -- 添加overall_sentiment_label字段
            topics TEXT,  -- 添加topics字段，存储为JSON字符串
            UNIQUE(ticker, title, date)
        )
        ''')
        
        self.conn.commit()
        
        # 更新表结构（添加新列）
        self._update_table_structure()
    
    def _update_table_structure(self):
        """更新表结构，添加新列"""
        cursor = self.conn.cursor()
        
        # 检查company_news表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='company_news'")
        if cursor.fetchone():
            # 检查overall_sentiment_label列是否存在
            cursor.execute("PRAGMA table_info(company_news)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # 添加缺失的列
            if 'overall_sentiment_label' not in columns:
                cursor.execute("ALTER TABLE company_news ADD COLUMN overall_sentiment_label TEXT")
                print("添加列: overall_sentiment_label")
            
            if 'topics' not in columns:
                cursor.execute("ALTER TABLE company_news ADD COLUMN topics TEXT")
                print("添加列: topics")
        
        self.conn.commit()
    
    def close(self):
        """关闭数据库连接"""
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
        except Exception:
            # 完全忽略关闭连接时的错误，不打印警告
            pass
    
    def __del__(self):
        """析构函数，确保数据库连接被关闭"""
        try:
            self.close()
        except Exception:
            # 忽略析构函数中的错误
            pass
    
    # 价格数据方法
    def set_prices(self, ticker, data):
        """存储价格数据"""
        cursor = self.conn.cursor()
        
        for item in data:
            # 准备插入的字段
            fields = ['ticker', 'time']
            values = [ticker, item.get('time')]
            
            # 动态添加其他字段
            for key, value in item.items():
                if key != 'time':  # 时间已经添加
                    fields.append(key)
                    values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO prices ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting price data: {e}")
        
        self.conn.commit()
    
    def get_prices(self, ticker, start_date=None, end_date=None):
        """获取价格数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM prices WHERE ticker = ?"
        params = [ticker]
        
        if start_date:
            sql += " AND time >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND time <= ?"
            params.append(end_date)
        
        sql += " ORDER BY time"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    # 财务指标方法
    def set_financial_metrics(self, ticker, data):
        """存储财务指标数据"""
        cursor = self.conn.cursor()
        
        for item in data:
            # 获取item的数据，支持字典和对象两种情况
            if hasattr(item, 'model_dump'):
                item_data = item.model_dump()
            elif hasattr(item, '__dict__'):
                item_data = item.__dict__
            else:
                item_data = item
            
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 动态添加其他字段
            for key, value in item_data.items():
                fields.append(key)
                values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO financial_metrics ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting financial metrics: {e}")
        
        self.conn.commit()
    
    def get_financial_metrics(self, ticker, report_period=None):
        """获取财务指标数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM financial_metrics WHERE ticker = ?"
        params = [ticker]
        
        if report_period:
            sql += " AND report_period = ?"
            params.append(report_period)
        
        sql += " ORDER BY report_period DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    # 财务项目方法
    def set_line_items(self, ticker, data):
        """存储财务项目数据"""
        cursor = self.conn.cursor()
        
        for item in data:
            # 获取item的数据，支持字典和对象两种情况
            if hasattr(item, 'model_dump'):
                item_data = item.model_dump()
            elif hasattr(item, '__dict__'):
                item_data = item.__dict__
            else:
                item_data = item
            
            # 获取基本信息
            report_period = item_data.get('report_period', datetime.now().strftime('%Y-%m-%d'))
            period = item_data.get('period', 'ttm')
            currency = item_data.get('currency', 'USD')
            
            # 遍历所有项目，存储为单独的记录
            for key, value in item_data.items():
                # 跳过基本信息字段
                if key in ['report_period', 'period', 'currency']:
                    continue
                
                # 插入数据
                sql = """
                INSERT OR REPLACE INTO line_items 
                (ticker, report_period, period, currency, item_name, item_value) 
                VALUES (?, ?, ?, ?, ?, ?)
                """
                
                try:
                    cursor.execute(sql, [ticker, report_period, period, currency, key, value])
                except Exception as e:
                    print(f"Error inserting line item: {e}")
        
        self.conn.commit()
    
    def get_line_items(self, ticker, report_period=None, item_names=None):
        """获取财务项目数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM line_items WHERE ticker = ?"
        params = [ticker]
        
        if report_period:
            sql += " AND report_period = ?"
            params.append(report_period)
        
        if item_names:
            placeholders = ', '.join(['?'] * len(item_names))
            sql += f" AND item_name IN ({placeholders})"
            params.extend(item_names)
        
        sql += " ORDER BY report_period DESC, item_name"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为按报告期分组的字典
        result = {}
        for row in rows:
            row_dict = dict(row)
            period = row_dict['report_period']
            
            if period not in result:
                result[period] = {
                    'ticker': ticker,
                    'report_period': period,
                    'period': row_dict['period'],
                    'currency': row_dict['currency']
                }
            
            # 添加项目值
            result[period][row_dict['item_name']] = row_dict['item_value']
        
        # 转换为列表
        return list(result.values())
    
    # 内部交易方法
    def set_insider_trades(self, ticker, data):
        """存储内部交易数据"""
        cursor = self.conn.cursor()
        
        for item in data:
            # 获取item的数据，支持字典和对象两种情况
            if hasattr(item, 'model_dump'):
                item_data = item.model_dump()
            elif hasattr(item, '__dict__'):
                item_data = item.__dict__
            else:
                item_data = item
            
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 确保transaction_date字段存在
            has_transaction_date = False
            
            # 动态添加其他字段
            for key, value in item_data.items():
                # 字段名映射
                if key == 'date':
                    fields.append('transaction_date')
                    has_transaction_date = True
                elif key == 'insider_name':
                    fields.append('name')
                elif key == 'insider_title':
                    fields.append('title')
                elif key == 'price':
                    fields.append('transaction_price_per_share')
                elif key == 'value':
                    fields.append('transaction_value')
                elif key == 'shares_owned':
                    fields.append('shares_owned_after_transaction')
                else:
                    fields.append(key)
                values.append(value)
            
            # 如果没有transaction_date字段，则使用当前日期
            if not has_transaction_date:
                fields.append('transaction_date')
                values.append(datetime.now().strftime('%Y-%m-%d'))
            
            # 确保filing_date字段存在
            if 'filing_date' not in fields:
                fields.append('filing_date')
                values.append(datetime.now().strftime('%Y-%m-%d'))
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO insider_trades ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting insider trade: {e}")
        
        self.conn.commit()
    
    def get_insider_trades(self, ticker, start_date=None, end_date=None):
        """获取内部交易数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM insider_trades WHERE ticker = ?"
        params = [ticker]
        
        if start_date:
            sql += " AND transaction_date >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND transaction_date <= ?"
            params.append(end_date)
        
        sql += " ORDER BY transaction_date DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            # 字段名映射回原始格式
            item['date'] = item.pop('transaction_date')
            if 'name' in item:
                item['insider_name'] = item.pop('name')
            if 'title' in item:
                item['insider_title'] = item.pop('title')
            if 'transaction_price_per_share' in item:
                item['price'] = item.pop('transaction_price_per_share')
            if 'transaction_value' in item:
                item['value'] = item.pop('transaction_value')
            if 'shares_owned_after_transaction' in item:
                item['shares_owned'] = item.pop('shares_owned_after_transaction')
            
            result.append(item)
        
        return result
    
    # 公司新闻方法
    def set_company_news(self, ticker, data):
        """存储公司新闻数据"""
        cursor = self.conn.cursor()
        
        for item in data:
            # 获取item的数据，支持字典和对象两种情况
            if hasattr(item, 'model_dump'):
                item_data = item.model_dump()
            elif hasattr(item, '__dict__'):
                item_data = item.__dict__
            else:
                item_data = item
            
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 动态添加其他字段
            for key, value in item_data.items():
                fields.append(key)
                values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO company_news ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting company news: {e}")
        
        self.conn.commit()
    
    def get_company_news(self, ticker, start_date=None, end_date=None):
        """获取公司新闻数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM company_news WHERE ticker = ?"
        params = [ticker]
        
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)
        
        sql += " ORDER BY date DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    # 辅助方法
    def execute_query(self, sql, params=None):
        """执行自定义SQL查询"""
        cursor = self.conn.cursor()
        
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    def execute_update(self, sql, params=None):
        """执行自定义SQL更新操作"""
        cursor = self.conn.cursor()
        
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        self.conn.commit()
        return cursor.rowcount
    
    def get_table_schema(self, table_name):
        """获取表结构信息"""
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return cursor.fetchall()
    
    def get_tables(self):
        """获取所有表名"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]
    
    def get_table_count(self, table_name):
        """获取表中的记录数"""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    
    def get_ticker_stats(self, ticker):
        """获取指定股票的统计信息"""
        stats = {}
        
        # 获取各表中该股票的记录数
        tables = ['prices', 'financial_metrics', 'line_items', 'insider_trades', 'company_news']
        for table in tables:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE ticker = ?", [ticker])
            stats[table] = cursor.fetchone()[0]
        
        # 获取价格数据的日期范围
        cursor = self.conn.cursor()
        cursor.execute("SELECT MIN(time), MAX(time) FROM prices WHERE ticker = ?", [ticker])
        date_range = cursor.fetchone()
        if date_range[0]:
            stats['price_date_range'] = {'start': date_range[0], 'end': date_range[1]}
        
        return stats
    
    def get_database_stats(self):
        """获取整个数据库的统计信息"""
        stats = {}
        
        # 获取各表的记录数
        tables = self.get_tables()
        for table in tables:
            stats[table] = self.get_table_count(table)
        
        # 获取所有股票代码
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM prices")
        tickers = [row[0] for row in cursor.fetchall()]
        stats['tickers'] = tickers
        stats['ticker_count'] = len(tickers)
        
        # 获取数据库文件大小
        if self.db_path.exists():
            stats['db_size'] = self.db_path.stat().st_size
        
        return stats

# 线程本地存储，为每个线程创建独立的数据库连接
_thread_local = threading.local()

def get_db():
    """获取当前线程的数据库实例"""
    if not hasattr(_thread_local, 'db'):
        _thread_local.db = Database()
    return _thread_local.db
