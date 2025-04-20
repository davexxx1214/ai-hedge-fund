"""
SQL工具模块，提供SQL查询和数据分析功能
"""

import pandas as pd
import sqlite3
from pathlib import Path
from src.data.database_core import get_db

class SQLTools:
    """SQL查询和数据分析工具类"""
    
    def __init__(self):
        """初始化SQL工具"""
        self.db = get_db()
    
    def query_to_df(self, sql, params=None):
        """执行SQL查询并返回DataFrame"""
        # 获取数据库连接
        conn = self.db.conn
        
        # 执行查询
        if params:
            df = pd.read_sql_query(sql, conn, params=params)
        else:
            df = pd.read_sql_query(sql, conn)
        
        return df
    
    def get_price_history(self, ticker, start_date=None, end_date=None):
        """获取股票价格历史数据"""
        sql = "SELECT * FROM prices WHERE ticker = ?"
        params = [ticker]
        
        if start_date:
            sql += " AND time >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND time <= ?"
            params.append(end_date)
        
        sql += " ORDER BY time"
        
        df = self.query_to_df(sql, params)
        if not df.empty and 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)
        
        return df
    
    def get_financial_metrics_history(self, ticker):
        """获取股票财务指标历史数据"""
        sql = "SELECT * FROM financial_metrics WHERE ticker = ? ORDER BY report_period DESC"
        params = [ticker]
        
        df = self.query_to_df(sql, params)
        if not df.empty and 'report_period' in df.columns:
            df['report_period'] = pd.to_datetime(df['report_period'])
            df.set_index('report_period', inplace=True)
        
        return df
    
    def get_line_items_pivot(self, ticker, items=None):
        """获取财务项目数据并透视为时间序列"""
        sql = """
        SELECT report_period, item_name, item_value 
        FROM line_items 
        WHERE ticker = ?
        """
        params = [ticker]
        
        if items:
            placeholders = ', '.join(['?'] * len(items))
            sql += f" AND item_name IN ({placeholders})"
            params.extend(items)
        
        sql += " ORDER BY report_period DESC"
        
        df = self.query_to_df(sql, params)
        if df.empty:
            return pd.DataFrame()
        
        # 透视表转换
        pivot_df = df.pivot(index='report_period', columns='item_name', values='item_value')
        pivot_df.index = pd.to_datetime(pivot_df.index)
        
        return pivot_df
    
    def get_insider_trades_summary(self, ticker, start_date=None, end_date=None):
        """获取内部交易汇总数据"""
        sql = """
        SELECT 
            transaction_date,
            SUM(CASE WHEN transaction_shares > 0 THEN transaction_shares ELSE 0 END) as buy_shares,
            SUM(CASE WHEN transaction_shares < 0 THEN ABS(transaction_shares) ELSE 0 END) as sell_shares,
            SUM(transaction_shares) as net_shares,
            SUM(transaction_value) as total_value,
            COUNT(*) as transaction_count
        FROM insider_trades
        WHERE ticker = ?
        """
        params = [ticker]
        
        if start_date:
            sql += " AND transaction_date >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND transaction_date <= ?"
            params.append(end_date)
        
        sql += " GROUP BY transaction_date ORDER BY transaction_date DESC"
        
        df = self.query_to_df(sql, params)
        if not df.empty and 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            df.set_index('transaction_date', inplace=True)
        
        return df
    
    def get_news_sentiment_trend(self, ticker, start_date=None, end_date=None):
        """获取新闻情感趋势数据"""
        # 由于数据库中的date字段可能是"YYYYMMDD"格式，而不是标准的"YYYY-MM-DD"格式
        # 我们需要转换日期格式或使用不同的比较方式
        
        # 方法1：不使用日期范围过滤，先获取所有数据，然后在Python中过滤
        sql = """
        SELECT 
            date,
            AVG(sentiment) as avg_sentiment,
            COUNT(*) as news_count
        FROM company_news
        WHERE ticker = ? AND sentiment IS NOT NULL
        GROUP BY date ORDER BY date
        """
        params = [ticker]
        
        df = self.query_to_df(sql, params)
        if not df.empty and 'date' in df.columns:
            # 尝试将日期转换为标准格式
            try:
                # 如果日期是"YYYYMMDD"格式，先转换为"YYYY-MM-DD"格式
                df['date_std'] = df['date'].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:8]}" if len(str(x)) == 8 and str(x).isdigit() else x)
                # 然后转换为datetime对象
                df['date'] = pd.to_datetime(df['date_std'])
                df = df.drop('date_std', axis=1)
            except Exception as e:
                print(f"日期转换错误: {e}")
                # 如果转换失败，尝试直接转换
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # 设置日期为索引
            df.set_index('date', inplace=True)
            
            # 在Python中过滤日期范围
            if start_date:
                start_date_dt = pd.to_datetime(start_date)
                df = df[df.index >= start_date_dt]
            
            if end_date:
                end_date_dt = pd.to_datetime(end_date)
                df = df[df.index <= end_date_dt]
        
        return df
    
    def get_stock_correlation(self, tickers, start_date=None, end_date=None):
        """计算多只股票之间的相关性"""
        # 获取每只股票的价格数据
        dfs = []
        for ticker in tickers:
            df = self.get_price_history(ticker, start_date, end_date)
            if not df.empty:
                # 只保留收盘价
                price_series = df['close']
                price_series.name = ticker
                dfs.append(price_series)
        
        # 合并数据
        if dfs:
            merged_df = pd.concat(dfs, axis=1)
            # 计算相关性
            correlation = merged_df.corr()
            return correlation
        
        return pd.DataFrame()
    
    def get_sector_performance(self, tickers, sector_mapping, start_date=None, end_date=None):
        """计算行业板块表现"""
        # 获取每只股票的价格数据
        stock_returns = {}
        for ticker in tickers:
            df = self.get_price_history(ticker, start_date, end_date)
            if not df.empty:
                # 计算收益率
                first_price = df['close'].iloc[0]
                last_price = df['close'].iloc[-1]
                returns = (last_price - first_price) / first_price
                stock_returns[ticker] = returns
        
        # 按行业分组
        sector_returns = {}
        for ticker, returns in stock_returns.items():
            sector = sector_mapping.get(ticker, 'Unknown')
            if sector not in sector_returns:
                sector_returns[sector] = []
            sector_returns[sector].append(returns)
        
        # 计算每个行业的平均收益率
        result = {}
        for sector, returns_list in sector_returns.items():
            result[sector] = sum(returns_list) / len(returns_list)
        
        return result
    
    def get_financial_ratios_comparison(self, tickers, ratios=None):
        """比较多只股票的财务比率"""
        if ratios is None:
            ratios = [
                'price_to_earnings_ratio', 
                'price_to_book_ratio', 
                'price_to_sales_ratio',
                'debt_to_equity',
                'return_on_equity',
                'net_margin'
            ]
        
        # 获取每只股票的最新财务指标
        data = {}
        for ticker in tickers:
            sql = f"""
            SELECT {', '.join(ratios)}
            FROM financial_metrics
            WHERE ticker = ?
            ORDER BY report_period DESC
            LIMIT 1
            """
            df = self.query_to_df(sql, [ticker])
            if not df.empty:
                data[ticker] = df.iloc[0].to_dict()
        
        # 转换为DataFrame
        result_df = pd.DataFrame.from_dict(data, orient='index')
        return result_df
    
    def get_growth_metrics(self, ticker, periods=4):
        """获取增长指标"""
        # 获取财务项目数据
        items = [
            'revenue', 
            'net_income', 
            'operating_income', 
            'free_cash_flow',
            'earnings_per_share'
        ]
        
        pivot_df = self.get_line_items_pivot(ticker, items)
        if pivot_df.empty:
            return pd.DataFrame()
        
        # 计算同比增长率
        growth_df = pivot_df.pct_change(periods=-1) * 100  # 负数表示与前一期比较
        
        # 计算复合年增长率 (CAGR)
        if len(pivot_df) >= periods:
            for col in pivot_df.columns:
                first_value = pivot_df[col].iloc[-periods]
                last_value = pivot_df[col].iloc[0]
                if first_value > 0:  # 避免除以零或负数
                    cagr = (last_value / first_value) ** (1 / periods) - 1
                    growth_df.loc['CAGR', col] = cagr * 100
        
        return growth_df
    
    def get_valuation_trend(self, ticker, metrics=None, periods=8):
        """获取估值趋势"""
        if metrics is None:
            metrics = [
                'price_to_earnings_ratio', 
                'price_to_book_ratio', 
                'price_to_sales_ratio',
                'enterprise_value_to_ebitda_ratio'
            ]
        
        # 获取财务指标历史数据
        sql = f"""
        SELECT report_period, {', '.join(metrics)}
        FROM financial_metrics
        WHERE ticker = ?
        ORDER BY report_period DESC
        LIMIT ?
        """
        
        df = self.query_to_df(sql, [ticker, periods])
        if not df.empty and 'report_period' in df.columns:
            df['report_period'] = pd.to_datetime(df['report_period'])
            df.set_index('report_period', inplace=True)
            df = df.sort_index()
        
        return df
    
    def get_database_summary(self):
        """获取数据库摘要信息"""
        # 获取数据库统计信息
        stats = self.db.get_database_stats()
        
        # 获取表结构信息
        tables = {}
        for table_name in self.db.get_tables():
            schema = self.db.get_table_schema(table_name)
            count = stats.get('table_counts', {}).get(table_name, 0)
            tables[table_name] = {
                'columns': [dict(row) for row in schema],
                'count': count
            }
        
        # 获取股票列表
        tickers = stats.get('distinct_tickers', [])
        
        return {
            'tables': tables,
            'tickers': tickers,
            'ticker_count': stats.get('distinct_ticker_count', 0),
            'db_size': stats.get('db_size_bytes', 0)
        }

# 全局SQL工具实例
_sql_tools = None

def get_sql_tools():
    """获取全局SQL工具实例"""
    global _sql_tools
    if _sql_tools is None:
        _sql_tools = SQLTools()
    return _sql_tools
