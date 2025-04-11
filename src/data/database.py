import os
import sqlite3
import json
import threading
from datetime import datetime
from pathlib import Path
import pandas as pd

# 使用线程本地存储
_thread_local = threading.local()

def get_db():
    """获取数据库连接，确保每个线程使用自己的连接"""
    if not hasattr(_thread_local, 'db'):
        # 为当前线程创建新的数据库连接
        db_path = Path("src/data/finance.db")
        _thread_local.db = Database(db_path)
        print(f"为线程 {threading.get_ident()} 创建新的数据库连接")
    return _thread_local.db

# 数据库文件路径
DB_PATH = Path("src/data/finance.db")

class Database:
    """SQLite数据库管理类，用于存储股票金融数据"""

    def __init__(self, db_path):
        """初始化数据库连接"""
        self.db_path = db_path
        self.conn = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """连接到数据库"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
    
    def _create_tables(self):
        """创建数据库表结构"""
        cursor = self.conn.cursor()
        
        # 创建公司概览表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS company_overview (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            last_updated TEXT NOT NULL,
            symbol TEXT,
            asset_type TEXT,
            name TEXT,
            description TEXT,
            cik TEXT,
            exchange TEXT,
            currency TEXT,
            country TEXT,
            sector TEXT,
            industry TEXT,
            address TEXT,
            official_site TEXT,
            fiscal_year_end TEXT,
            latest_quarter TEXT,
            market_capitalization REAL,
            ebitda REAL,
            pe_ratio REAL,
            peg_ratio REAL,
            book_value REAL,
            dividend_per_share REAL,
            dividend_yield REAL,
            eps REAL,
            revenue_per_share_ttm REAL,
            profit_margin REAL,
            operating_margin_ttm REAL,
            return_on_assets_ttm REAL,
            return_on_equity_ttm REAL,
            revenue_ttm REAL,
            gross_profit_ttm REAL,
            diluted_eps_ttm REAL,
            quarterly_earnings_growth_yoy REAL,
            quarterly_revenue_growth_yoy REAL,
            analyst_target_price REAL,
            analyst_rating_strong_buy INTEGER,
            analyst_rating_buy INTEGER,
            analyst_rating_hold INTEGER,
            analyst_rating_sell INTEGER,
            analyst_rating_strong_sell INTEGER,
            trailing_pe REAL,
            forward_pe REAL,
            price_to_sales_ratio_ttm REAL,
            price_to_book_ratio REAL,
            ev_to_revenue REAL,
            ev_to_ebitda REAL,
            beta REAL,
            week_52_high REAL,
            week_52_low REAL,
            day_50_moving_average REAL,
            day_200_moving_average REAL,
            shares_outstanding REAL,
            dividend_date TEXT,
            ex_dividend_date TEXT,
            UNIQUE(ticker)
        )
        ''')
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
        
        
        # 创建利润表（年报）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS income_statement_annual (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            fiscalDateEnding TEXT NOT NULL,
            reportedCurrency TEXT,
            grossProfit REAL,
            totalRevenue REAL,
            costOfRevenue REAL,
            costofGoodsAndServicesSold REAL,
            operatingIncome REAL,
            sellingGeneralAndAdministrative REAL,
            researchAndDevelopment REAL,
            operatingExpenses REAL,
            investmentIncomeNet REAL,
            netInterestIncome REAL,
            interestIncome REAL,
            interestExpense REAL,
            nonInterestIncome REAL,
            otherNonOperatingIncome REAL,
            depreciation REAL,
            depreciationAndAmortization REAL,
            incomeBeforeTax REAL,
            incomeTaxExpense REAL,
            interestAndDebtExpense REAL,
            netIncomeFromContinuingOperations REAL,
            comprehensiveIncomeNetOfTax REAL,
            ebit REAL,
            ebitda REAL,
            netIncome REAL,
            UNIQUE(ticker, fiscalDateEnding)
        )
        ''')
        
        # 创建资产负债表（年报）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS balance_sheet_annual (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            fiscalDateEnding TEXT NOT NULL,
            reportedCurrency TEXT,
            totalAssets REAL,
            totalCurrentAssets REAL,
            cashAndCashEquivalentsAtCarryingValue REAL,
            cashAndShortTermInvestments REAL,
            inventory REAL,
            currentNetReceivables REAL,
            totalNonCurrentAssets REAL,
            propertyPlantEquipment REAL,
            accumulatedDepreciationAmortizationPPE REAL,
            intangibleAssets REAL,
            intangibleAssetsExcludingGoodwill REAL,
            goodwill REAL,
            investments REAL,
            longTermInvestments REAL,
            shortTermInvestments REAL,
            otherCurrentAssets REAL,
            otherNonCurrentAssets REAL,
            totalLiabilities REAL,
            totalCurrentLiabilities REAL,
            currentAccountsPayable REAL,
            deferredRevenue REAL,
            currentDebt REAL,
            shortTermDebt REAL,
            totalNonCurrentLiabilities REAL,
            capitalLeaseObligations REAL,
            longTermDebt REAL,
            currentLongTermDebt REAL,
            longTermDebtNoncurrent REAL,
            shortLongTermDebtTotal REAL,
            otherCurrentLiabilities REAL,
            otherNonCurrentLiabilities REAL,
            totalShareholderEquity REAL,
            treasuryStock REAL,
            retainedEarnings REAL,
            commonStock REAL,
            commonStockSharesOutstanding REAL,
            UNIQUE(ticker, fiscalDateEnding)
        )
        ''')
        
        # 创建现金流量表（年报）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cash_flow_annual (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            fiscalDateEnding TEXT NOT NULL,
            reportedCurrency TEXT,
            operatingCashflow REAL,
            paymentsForOperatingActivities REAL,
            proceedsFromOperatingActivities REAL,
            changeInOperatingLiabilities REAL,
            changeInOperatingAssets REAL,
            depreciationDepletionAndAmortization REAL,
            capitalExpenditures REAL,
            changeInReceivables REAL,
            changeInInventory REAL,
            profitLoss REAL,
            cashflowFromInvestment REAL,
            cashflowFromFinancing REAL,
            proceedsFromRepaymentsOfShortTermDebt REAL,
            paymentsForRepurchaseOfCommonStock REAL,
            paymentsForRepurchaseOfEquity REAL,
            paymentsForRepurchaseOfPreferredStock REAL,
            dividendPayout REAL,
            dividendPayoutCommonStock REAL,
            dividendPayoutPreferredStock REAL,
            proceedsFromIssuanceOfCommonStock REAL,
            proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet REAL,
            proceedsFromIssuanceOfPreferredStock REAL,
            proceedsFromRepurchaseOfEquity REAL,
            proceedsFromSaleOfTreasuryStock REAL,
            changeInCashAndCashEquivalents REAL,
            changeInExchangeRate REAL,
            netIncome REAL,
            UNIQUE(ticker, fiscalDateEnding)
        )
        ''')
        
        # 创建利润表（季度）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS income_statement_quarterly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            fiscalDateEnding TEXT NOT NULL,
            reportedCurrency TEXT,
            grossProfit REAL,
            totalRevenue REAL,
            costOfRevenue REAL,
            costofGoodsAndServicesSold REAL,
            operatingIncome REAL,
            sellingGeneralAndAdministrative REAL,
            researchAndDevelopment REAL,
            operatingExpenses REAL,
            investmentIncomeNet REAL,
            netInterestIncome REAL,
            interestIncome REAL,
            interestExpense REAL,
            nonInterestIncome REAL,
            otherNonOperatingIncome REAL,
            depreciation REAL,
            depreciationAndAmortization REAL,
            incomeBeforeTax REAL,
            incomeTaxExpense REAL,
            interestAndDebtExpense REAL,
            netIncomeFromContinuingOperations REAL,
            comprehensiveIncomeNetOfTax REAL,
            ebit REAL,
            ebitda REAL,
            netIncome REAL,
            UNIQUE(ticker, fiscalDateEnding)
        )
        ''')
        
        # 创建资产负债表（季度）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS balance_sheet_quarterly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            fiscalDateEnding TEXT NOT NULL,
            reportedCurrency TEXT,
            totalAssets REAL,
            totalCurrentAssets REAL,
            cashAndCashEquivalentsAtCarryingValue REAL,
            cashAndShortTermInvestments REAL,
            inventory REAL,
            currentNetReceivables REAL,
            totalNonCurrentAssets REAL,
            propertyPlantEquipment REAL,
            accumulatedDepreciationAmortizationPPE REAL,
            intangibleAssets REAL,
            intangibleAssetsExcludingGoodwill REAL,
            goodwill REAL,
            investments REAL,
            longTermInvestments REAL,
            shortTermInvestments REAL,
            otherCurrentAssets REAL,
            otherNonCurrentAssets REAL,
            totalLiabilities REAL,
            totalCurrentLiabilities REAL,
            currentAccountsPayable REAL,
            deferredRevenue REAL,
            currentDebt REAL,
            shortTermDebt REAL,
            totalNonCurrentLiabilities REAL,
            capitalLeaseObligations REAL,
            longTermDebt REAL,
            currentLongTermDebt REAL,
            longTermDebtNoncurrent REAL,
            shortLongTermDebtTotal REAL,
            otherCurrentLiabilities REAL,
            otherNonCurrentLiabilities REAL,
            totalShareholderEquity REAL,
            treasuryStock REAL,
            retainedEarnings REAL,
            commonStock REAL,
            commonStockSharesOutstanding REAL,
            UNIQUE(ticker, fiscalDateEnding)
        )
        ''')
        
        # 创建现金流量表（季度）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cash_flow_quarterly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            fiscalDateEnding TEXT NOT NULL,
            reportedCurrency TEXT,
            operatingCashflow REAL,
            paymentsForOperatingActivities REAL,
            proceedsFromOperatingActivities REAL,
            changeInOperatingLiabilities REAL,
            changeInOperatingAssets REAL,
            depreciationDepletionAndAmortization REAL,
            capitalExpenditures REAL,
            changeInReceivables REAL,
            changeInInventory REAL,
            profitLoss REAL,
            cashflowFromInvestment REAL,
            cashflowFromFinancing REAL,
            proceedsFromRepaymentsOfShortTermDebt REAL,
            paymentsForRepurchaseOfCommonStock REAL,
            paymentsForRepurchaseOfEquity REAL,
            paymentsForRepurchaseOfPreferredStock REAL,
            dividendPayout REAL,
            dividendPayoutCommonStock REAL,
            dividendPayoutPreferredStock REAL,
            proceedsFromIssuanceOfCommonStock REAL,
            proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet REAL,
            proceedsFromIssuanceOfPreferredStock REAL,
            proceedsFromRepurchaseOfEquity REAL,
            proceedsFromSaleOfTreasuryStock REAL,
            changeInCashAndCashEquivalents REAL,
            changeInExchangeRate REAL,
            netIncome REAL,
            UNIQUE(ticker, fiscalDateEnding)
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
        
        # 创建公司新闻表 (修订版)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS company_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            url TEXT NOT NULL,
            date TEXT NOT NULL,
            time_published_raw TEXT, -- 存储原始的 time_published 字段
            title TEXT,
            summary TEXT,
            sentiment_score REAL,
            sentiment_label TEXT, -- 对应 overall_sentiment_label
            author TEXT, -- 对应 CompanyNews.author (处理后的 authors 列表)
            topics TEXT, -- 对应 CompanyNews.topics (处理后的 topics 列表)
            source_domain TEXT,
            banner_image TEXT,
            category_within_source TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, url) -- 使用 ticker 和 url 作为唯一约束
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
            
            # --- 更新表结构逻辑调整 ---
            # (移除错误的 ADD COLUMN overall_sentiment_label 行)

            # 检查 url 列是否存在
            if 'url' not in columns:
                try:
                    cursor.execute("ALTER TABLE company_news ADD COLUMN url TEXT")
                    print("添加列: url")
                except sqlite3.OperationalError as e:
                    print(f"添加 url 列失败 (可能已存在): {e}")

            # 检查 sentiment_score 列是否存在 (替换旧的 sentiment)
            if 'sentiment_score' not in columns and 'sentiment' in columns:
                 try:
                    cursor.execute("ALTER TABLE company_news RENAME COLUMN sentiment TO sentiment_score")
                    print("重命名列: sentiment -> sentiment_score")
                 except sqlite3.OperationalError as e:
                    print(f"重命名 sentiment 列失败: {e}")
            elif 'sentiment_score' not in columns:
                 try:
                    cursor.execute("ALTER TABLE company_news ADD COLUMN sentiment_score REAL")
                    print("添加列: sentiment_score")
                 except sqlite3.OperationalError as e:
                    print(f"添加 sentiment_score 列失败: {e}")

            # 检查 sentiment_label 列是否存在 (替换旧的 overall_sentiment_label)
            if 'sentiment_label' not in columns and 'overall_sentiment_label' in columns:
                 try:
                    cursor.execute("ALTER TABLE company_news RENAME COLUMN overall_sentiment_label TO sentiment_label")
                    print("重命名列: overall_sentiment_label -> sentiment_label")
                 except sqlite3.OperationalError as e:
                    print(f"重命名 overall_sentiment_label 列失败: {e}")
            elif 'sentiment_label' not in columns:
                 try:
                    cursor.execute("ALTER TABLE company_news ADD COLUMN sentiment_label TEXT")
                    print("添加列: sentiment_label")
                 except sqlite3.OperationalError as e:
                    print(f"添加 sentiment_label 列失败: {e}")

            # 检查 time_published_raw 列
            if 'time_published_raw' not in columns:
                 try:
                    cursor.execute("ALTER TABLE company_news ADD COLUMN time_published_raw TEXT")
                    print("添加列: time_published_raw")
                 except sqlite3.OperationalError as e:
                    print(f"添加 time_published_raw 列失败: {e}")

            # 检查 fetched_at 列
            if 'fetched_at' not in columns:
                 try:
                    cursor.execute("ALTER TABLE company_news ADD COLUMN fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    print("添加列: fetched_at")
                 except sqlite3.OperationalError as e:
                    print(f"添加 fetched_at 列失败: {e}")
            
            # 检查并可能重建唯一索引 (如果旧的 UNIQUE(ticker, title, date) 存在)
            # 注意：直接修改 UNIQUE 约束比较复杂，通常需要重建表。
            # 这里简化处理，假设旧约束不存在或不影响新约束的添加。
            # 如果需要严格处理，需要更复杂的迁移逻辑。
            try:
                # 尝试创建新的唯一索引 (如果不存在)
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_company_news_ticker_url ON company_news (ticker, url)")
                print("确保唯一索引 idx_company_news_ticker_url 存在")
            except sqlite3.OperationalError as e:
                print(f"创建唯一索引 idx_company_news_ticker_url 失败 (可能已存在或冲突): {e}")

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
    
    def set_company_overview(self, ticker, data):
        """存储公司概览数据"""
        cursor = self.conn.cursor()
        
        # 准备数据
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 字段映射，将API返回的字段名转换为数据库字段名
        field_mapping = {
            'Symbol': 'symbol',
            'AssetType': 'asset_type',
            'Name': 'name',
            'Description': 'description',
            'CIK': 'cik',
            'Exchange': 'exchange',
            'Currency': 'currency',
            'Country': 'country',
            'Sector': 'sector',
            'Industry': 'industry',
            'Address': 'address',
            'OfficialSite': 'official_site',
            'FiscalYearEnd': 'fiscal_year_end',
            'LatestQuarter': 'latest_quarter',
            'MarketCapitalization': 'market_capitalization',
            'EBITDA': 'ebitda',
            'PERatio': 'pe_ratio',
            'PEGRatio': 'peg_ratio',
            'BookValue': 'book_value',
            'DividendPerShare': 'dividend_per_share',
            'DividendYield': 'dividend_yield',
            'EPS': 'eps',
            'RevenuePerShareTTM': 'revenue_per_share_ttm',
            'ProfitMargin': 'profit_margin',
            'OperatingMarginTTM': 'operating_margin_ttm',
            'ReturnOnAssetsTTM': 'return_on_assets_ttm',
            'ReturnOnEquityTTM': 'return_on_equity_ttm',
            'RevenueTTM': 'revenue_ttm',
            'GrossProfitTTM': 'gross_profit_ttm',
            'DilutedEPSTTM': 'diluted_eps_ttm',
            'QuarterlyEarningsGrowthYOY': 'quarterly_earnings_growth_yoy',
            'QuarterlyRevenueGrowthYOY': 'quarterly_revenue_growth_yoy',
            'AnalystTargetPrice': 'analyst_target_price',
            'AnalystRatingStrongBuy': 'analyst_rating_strong_buy',
            'AnalystRatingBuy': 'analyst_rating_buy',
            'AnalystRatingHold': 'analyst_rating_hold',
            'AnalystRatingSell': 'analyst_rating_sell',
            'AnalystRatingStrongSell': 'analyst_rating_strong_sell',
            'TrailingPE': 'trailing_pe',
            'ForwardPE': 'forward_pe',
            'PriceToSalesRatioTTM': 'price_to_sales_ratio_ttm',
            'PriceToBookRatio': 'price_to_book_ratio',
            'EVToRevenue': 'ev_to_revenue',
            'EVToEBITDA': 'ev_to_ebitda',
            'Beta': 'beta',
            '52WeekHigh': 'week_52_high',
            '52WeekLow': 'week_52_low',
            '50DayMovingAverage': 'day_50_moving_average',
            '200DayMovingAverage': 'day_200_moving_average',
            'SharesOutstanding': 'shares_outstanding',
            'DividendDate': 'dividend_date',
            'ExDividendDate': 'ex_dividend_date'
        }
        
        # 准备插入的字段和值
        fields = ['ticker', 'last_updated']
        values = [ticker, now]
        
        # 动态添加其他字段
        for api_field, db_field in field_mapping.items():
            if api_field in data:
                fields.append(db_field)
                # 尝试将数值字段转换为浮点数
                if api_field in ['MarketCapitalization', 'EBITDA', 'PERatio', 'PEGRatio', 
                                'BookValue', 'DividendPerShare', 'DividendYield', 'EPS',
                                'RevenuePerShareTTM', 'ProfitMargin', 'OperatingMarginTTM',
                                'ReturnOnAssetsTTM', 'ReturnOnEquityTTM', 'RevenueTTM',
                                'GrossProfitTTM', 'DilutedEPSTTM', 'QuarterlyEarningsGrowthYOY',
                                'QuarterlyRevenueGrowthYOY', 'AnalystTargetPrice',
                                'AnalystRatingStrongBuy', 'AnalystRatingBuy', 'AnalystRatingHold',
                                'AnalystRatingSell', 'AnalystRatingStrongSell',
                                'TrailingPE', 'ForwardPE', 'PriceToSalesRatioTTM',
                                'PriceToBookRatio', 'EVToRevenue', 'EVToEBITDA',
                                'Beta', '52WeekHigh', '52WeekLow', '50DayMovingAverage',
                                '200DayMovingAverage', 'SharesOutstanding']:
                    try:
                        values.append(float(data[api_field]))
                    except (ValueError, TypeError):
                        values.append(None)
                else:
                    values.append(data[api_field])
        
        # 构建SQL语句
        placeholders = ', '.join(['?'] * len(fields))
        fields_str = ', '.join(fields)
        
        # 使用INSERT OR REPLACE确保唯一性
        sql = f"INSERT OR REPLACE INTO company_overview ({fields_str}) VALUES ({placeholders})"
        
        try:
            cursor.execute(sql, values)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting company overview data: {e}")
            return False

    def get_company_overview(self, ticker):
        """获取公司概览数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM company_overview WHERE ticker = ?"
        params = [ticker]
        
        cursor.execute(sql, params)
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        else:
            return None

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
    
    # 利润表（年报）方法
    def set_income_statement_annual(self, ticker, data):
        """存储年度利润表数据"""
        cursor = self.conn.cursor()
        
        # --- Removed incorrect hasattr check ---
        # data should already be list[dict] as per caller (api_financials.py)
        data_list = data
        # --- End Removal ---

        # 允许的字段，避免非法字段导致错误
        allowed_fields = {
            'fiscalDateEnding', 'reportedCurrency', 'grossProfit', 'totalRevenue', 'costOfRevenue',
            'costofGoodsAndServicesSold', 'operatingIncome', 'sellingGeneralAndAdministrative',
            'researchAndDevelopment', 'operatingExpenses', 'investmentIncomeNet', 'netInterestIncome',
            'interestIncome', 'interestExpense', 'nonInterestIncome', 'otherNonOperatingIncome',
            'depreciation', 'depreciationAndAmortization', 'incomeBeforeTax', 'incomeTaxExpense',
            'interestAndDebtExpense', 'netIncomeFromContinuingOperations', 'comprehensiveIncomeNetOfTax',
            'ebit', 'ebitda', 'netIncome'
        }
        
        for item in data_list:
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 动态添加其他字段，过滤非法字段
            for key, value in item.items():
                if key not in allowed_fields:
                    continue
                # 复杂类型转字符串
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False)
                fields.append(key)
                values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO income_statement_annual ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting income statement annual: {e}\nSQL: {sql}\nValues: {values}")
        
        self.conn.commit()
    
    def get_income_statement_annual(self, ticker, fiscal_date_ending=None):
        """获取年度利润表数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM income_statement_annual WHERE ticker = ?"
        params = [ticker]
        
        if fiscal_date_ending:
            sql += " AND fiscalDateEnding = ?"
            params.append(fiscal_date_ending)
        
        sql += " ORDER BY fiscalDateEnding DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    # 资产负债表（年报）方法
    def set_balance_sheet_annual(self, ticker, data):
        """存储年度资产负债表数据"""
        cursor = self.conn.cursor()
        
        # --- Removed incorrect hasattr check ---
        # data should already be list[dict] as per caller (api_financials.py)
        data_list = data
        # --- End Removal ---
        
        for item in data_list:
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 动态添加其他字段
            for key, value in item.items():
                fields.append(key)
                values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO balance_sheet_annual ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting balance sheet annual: {e}")
        
        self.conn.commit()
    
    def get_balance_sheet_annual(self, ticker, fiscal_date_ending=None):
        """获取年度资产负债表数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM balance_sheet_annual WHERE ticker = ?"
        params = [ticker]
        
        if fiscal_date_ending:
            sql += " AND fiscalDateEnding = ?"
            params.append(fiscal_date_ending)
        
        sql += " ORDER BY fiscalDateEnding DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    # 现金流量表（年报）方法
    def set_cash_flow_annual(self, ticker, data):
        """存储年度现金流量表数据"""
        cursor = self.conn.cursor()
        
        # --- Removed incorrect hasattr check ---
        # data should already be list[dict] as per caller (api_financials.py)
        data_list = data
        # --- End Removal ---
        
        for item in data_list:
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 动态添加其他字段
            for key, value in item.items():
                fields.append(key)
                values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO cash_flow_annual ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting cash flow annual: {e}")
        
        self.conn.commit()
    
    def get_cash_flow_annual(self, ticker, fiscal_date_ending=None):
        """获取年度现金流量表数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM cash_flow_annual WHERE ticker = ?"
        params = [ticker]
        
        if fiscal_date_ending:
            sql += " AND fiscalDateEnding = ?"
            params.append(fiscal_date_ending)
        
        sql += " ORDER BY fiscalDateEnding DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    # 利润表（季度）方法
    def set_income_statement_quarterly(self, ticker, data):
        """存储季度利润表数据"""
        cursor = self.conn.cursor()
        
        # --- Removed incorrect hasattr check ---
        # data should already be list[dict] as per caller (api_financials.py)
        data_list = data
        # --- End Removal ---
        
        for item in data_list:
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 动态添加其他字段
            for key, value in item.items():
                fields.append(key)
                values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO income_statement_quarterly ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting income statement quarterly: {e}")
        
        self.conn.commit()
    
    def get_income_statement_quarterly(self, ticker, fiscal_date_ending=None):
        """获取季度利润表数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM income_statement_quarterly WHERE ticker = ?"
        params = [ticker]
        
        if fiscal_date_ending:
            sql += " AND fiscalDateEnding = ?"
            params.append(fiscal_date_ending)
        
        sql += " ORDER BY fiscalDateEnding DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    # 资产负债表（季度）方法
    def set_balance_sheet_quarterly(self, ticker, data):
        """存储季度资产负债表数据"""
        cursor = self.conn.cursor()
        
        # --- Removed incorrect hasattr check ---
        # data should already be list[dict] as per caller (api_financials.py)
        data_list = data
        # --- End Removal ---
        
        for item in data_list:
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 动态添加其他字段
            for key, value in item.items():
                fields.append(key)
                values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO balance_sheet_quarterly ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting balance sheet quarterly: {e}")
        
        self.conn.commit()
    
    def get_balance_sheet_quarterly(self, ticker, fiscal_date_ending=None):
        """获取季度资产负债表数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM balance_sheet_quarterly WHERE ticker = ?"
        params = [ticker]
        
        if fiscal_date_ending:
            sql += " AND fiscalDateEnding = ?"
            params.append(fiscal_date_ending)
        
        sql += " ORDER BY fiscalDateEnding DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
    # 现金流量表（季度）方法
    def set_cash_flow_quarterly(self, ticker, data):
        """存储季度现金流量表数据"""
        cursor = self.conn.cursor()
        
        # --- Removed incorrect hasattr check ---
        # data should already be list[dict] as per caller (api_financials.py)
        data_list = data
        # --- End Removal ---
        
        for item in data_list:
            # 准备插入的字段
            fields = ['ticker']
            values = [ticker]
            
            # 动态添加其他字段
            for key, value in item.items():
                fields.append(key)
                values.append(value)
            
            # 构建SQL语句
            placeholders = ', '.join(['?'] * len(fields))
            fields_str = ', '.join(fields)
            
            # 使用INSERT OR REPLACE确保唯一性
            sql = f"INSERT OR REPLACE INTO cash_flow_quarterly ({fields_str}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
            except Exception as e:
                print(f"Error inserting cash flow quarterly: {e}")
        
        self.conn.commit()
    
    def get_cash_flow_quarterly(self, ticker, fiscal_date_ending=None):
        """获取季度现金流量表数据"""
        cursor = self.conn.cursor()
        
        sql = "SELECT * FROM cash_flow_quarterly WHERE ticker = ?"
        params = [ticker]
        
        if fiscal_date_ending:
            sql += " AND fiscalDateEnding = ?"
            params.append(fiscal_date_ending)
        
        sql += " ORDER BY fiscalDateEnding DESC"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 转换为字典列表
        result = []
        for row in rows:
            item = dict(row)
            result.append(item)
        
        return result
    
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
    
    # 公司新闻方法 (修订版)
    def set_company_news(self, ticker: str, news_list: list):
        """
        存储公司新闻数据 (来自 CompanyNews 对象列表) 到数据库。
        使用 INSERT OR IGNORE 避免插入重复记录 (基于 ticker 和 url)。
        """
        if not news_list:
            return

        cursor = self.conn.cursor()
        insert_sql = """
        INSERT OR IGNORE INTO company_news (
            ticker, url, date, time_published_raw, title, summary, 
            sentiment_score, sentiment_label, author, topics, 
            source_domain, banner_image, category_within_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        rows_to_insert = []
        for news_item in news_list:
            # 确保是 CompanyNews 对象或类似结构
            if not hasattr(news_item, 'url') or not news_item.url:
                print(f"Skipping news item for {ticker} due to missing URL: {getattr(news_item, 'title', 'N/A')}")
                continue

            # 从 CompanyNews 对象提取数据
            # 注意：需要访问原始 time_published，但 CompanyNews 类目前不直接存储它
            # 我们需要从原始 kwargs 获取，或者修改 CompanyNews 类
            # 暂时假设 news_item.__dict__ 包含原始 kwargs 或类似信息
            raw_data = news_item.__dict__ # 这是一个简化假设
            
            row = (
                ticker,
                getattr(news_item, 'url', None), # 使用 getattr 以防万一
                getattr(news_item, 'date', None),
                raw_data.get('time_published', None), # 尝试获取原始 time_published
                getattr(news_item, 'title', None),
                getattr(news_item, 'summary', None),
                getattr(news_item, 'sentiment', None), # 对应 sentiment_score
                getattr(news_item, 'overall_sentiment_label', None), # 对应 sentiment_label
                getattr(news_item, 'author', None),
                getattr(news_item, 'topics', None),
                getattr(news_item, 'source_domain', None),
                getattr(news_item, 'banner_image', None),
                getattr(news_item, 'category_within_source', None)
            )
            rows_to_insert.append(row)

        if rows_to_insert:
            try:
                cursor.executemany(insert_sql, rows_to_insert)
                self.conn.commit()
                print(f"成功插入或忽略了 {len(rows_to_insert)} 条 {ticker} 的新闻记录。")
            except sqlite3.Error as e:
                print(f"批量插入 {ticker} 新闻数据时出错: {e}")
                self.conn.rollback() # 出错时回滚
            except Exception as e:
                print(f"处理 {ticker} 新闻数据时发生意外错误: {e}")
                self.conn.rollback()

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
    """获取数据库连接，确保每个线程使用自己的连接"""
    if not hasattr(_thread_local, 'db'):
        # 为当前线程创建新的数据库连接
        db_path = Path("src/data/finance.db")
        _thread_local.db = Database(db_path)
        print(f"为线程 {threading.get_ident()} 创建新的数据库连接")
    return _thread_local.db
