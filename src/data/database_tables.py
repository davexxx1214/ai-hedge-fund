import sqlite3

# Define SQL statements as constants for clarity
CREATE_COMPANY_OVERVIEW_SQL = '''
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
'''

CREATE_PRICES_SQL = '''
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
'''

CREATE_INCOME_STATEMENT_ANNUAL_SQL = '''
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
'''

CREATE_BALANCE_SHEET_ANNUAL_SQL = '''
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
'''

CREATE_CASH_FLOW_ANNUAL_SQL = '''
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
'''

CREATE_INCOME_STATEMENT_QUARTERLY_SQL = '''
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
'''

CREATE_BALANCE_SHEET_QUARTERLY_SQL = '''
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
'''

CREATE_CASH_FLOW_QUARTERLY_SQL = '''
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
'''

CREATE_INSIDER_TRADES_SQL = '''
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
'''

CREATE_COMPANY_NEWS_SQL = '''
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
'''

class DatabaseTablesMixin:
    """Mixin class for database table creation and updates."""

    def _create_tables(self):
        """创建数据库表结构"""
        if not self.conn:
            print("错误：数据库连接未建立，无法创建表。")
            return
        try:
            cursor = self.conn.cursor()

            # 创建所有表
            cursor.execute(CREATE_COMPANY_OVERVIEW_SQL)
            cursor.execute(CREATE_PRICES_SQL)
            cursor.execute(CREATE_INCOME_STATEMENT_ANNUAL_SQL)
            cursor.execute(CREATE_BALANCE_SHEET_ANNUAL_SQL)
            cursor.execute(CREATE_CASH_FLOW_ANNUAL_SQL)
            cursor.execute(CREATE_INCOME_STATEMENT_QUARTERLY_SQL)
            cursor.execute(CREATE_BALANCE_SHEET_QUARTERLY_SQL)
            cursor.execute(CREATE_CASH_FLOW_QUARTERLY_SQL)
            cursor.execute(CREATE_INSIDER_TRADES_SQL)
            cursor.execute(CREATE_COMPANY_NEWS_SQL)

            self.conn.commit()
            print("数据库表结构已创建或确认存在。")
        except sqlite3.Error as e:
            print(f"创建数据库表时出错: {e}")
            self.conn.rollback() # 出错时回滚

    def _update_table_structure(self):
        """更新表结构，添加新列 (主要针对 company_news 表)"""
        if not self.conn:
            print("错误：数据库连接未建立，无法更新表结构。")
            return

        cursor = self.conn.cursor()

        try:
            # 检查company_news表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='company_news'")
            if cursor.fetchone():
                # 获取现有列
                cursor.execute("PRAGMA table_info(company_news)")
                columns = {column[1] for column in cursor.fetchall()} # Use a set for faster lookups

                # --- 更新 company_news 表结构逻辑 ---
                updates_made = False

                # 检查 url 列是否存在 (理论上 CREATE TABLE 已经处理，但作为健壮性检查)
                if 'url' not in columns:
                    try:
                        cursor.execute("ALTER TABLE company_news ADD COLUMN url TEXT")
                        print("添加列: url")
                        updates_made = True
                    except sqlite3.OperationalError as e:
                        print(f"添加 url 列失败 (可能已存在): {e}")

                # 检查 sentiment_score 列是否存在 (替换旧的 sentiment)
                if 'sentiment_score' not in columns:
                    if 'sentiment' in columns:
                        try:
                            # SQLite < 3.25 不支持 RENAME COLUMN，需要更复杂操作
                            # 假设使用的是较新版本或此列不存在
                            cursor.execute("ALTER TABLE company_news RENAME COLUMN sentiment TO sentiment_score")
                            print("重命名列: sentiment -> sentiment_score")
                            updates_made = True
                        except sqlite3.OperationalError as e:
                            print(f"重命名 sentiment 列失败 (可能不支持或已存在): {e}")
                            # 如果重命名失败，尝试添加新列
                            try:
                                cursor.execute("ALTER TABLE company_news ADD COLUMN sentiment_score REAL")
                                print("添加列: sentiment_score")
                                updates_made = True
                            except sqlite3.OperationalError as e_add:
                                print(f"添加 sentiment_score 列也失败: {e_add}")
                    else:
                        try:
                            cursor.execute("ALTER TABLE company_news ADD COLUMN sentiment_score REAL")
                            print("添加列: sentiment_score")
                            updates_made = True
                        except sqlite3.OperationalError as e:
                            print(f"添加 sentiment_score 列失败: {e}")

                # 检查 sentiment_label 列是否存在 (替换旧的 overall_sentiment_label)
                if 'sentiment_label' not in columns:
                    if 'overall_sentiment_label' in columns:
                        try:
                            cursor.execute("ALTER TABLE company_news RENAME COLUMN overall_sentiment_label TO sentiment_label")
                            print("重命名列: overall_sentiment_label -> sentiment_label")
                            updates_made = True
                        except sqlite3.OperationalError as e:
                            print(f"重命名 overall_sentiment_label 列失败 (可能不支持或已存在): {e}")
                            try:
                                cursor.execute("ALTER TABLE company_news ADD COLUMN sentiment_label TEXT")
                                print("添加列: sentiment_label")
                                updates_made = True
                            except sqlite3.OperationalError as e_add:
                                print(f"添加 sentiment_label 列也失败: {e_add}")
                    else:
                        try:
                            cursor.execute("ALTER TABLE company_news ADD COLUMN sentiment_label TEXT")
                            print("添加列: sentiment_label")
                            updates_made = True
                        except sqlite3.OperationalError as e:
                            print(f"添加 sentiment_label 列失败: {e}")

                # 检查 time_published_raw 列
                if 'time_published_raw' not in columns:
                    try:
                        cursor.execute("ALTER TABLE company_news ADD COLUMN time_published_raw TEXT")
                        print("添加列: time_published_raw")
                        updates_made = True
                    except sqlite3.OperationalError as e:
                        print(f"添加 time_published_raw 列失败: {e}")

                # 检查 fetched_at 列
                if 'fetched_at' not in columns:
                    try:
                        cursor.execute("ALTER TABLE company_news ADD COLUMN fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                        print("添加列: fetched_at")
                        updates_made = True
                    except sqlite3.OperationalError as e:
                        print(f"添加 fetched_at 列失败: {e}")

                # 检查并可能重建唯一索引 (如果旧的 UNIQUE(ticker, title, date) 存在)
                # 注意：直接修改 UNIQUE 约束比较复杂，通常需要重建表。
                # 这里简化处理，假设旧约束不存在或不影响新约束的添加。
                try:
                    # 尝试创建新的唯一索引 (如果不存在)
                    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_company_news_ticker_url ON company_news (ticker, url)")
                    # print("确保唯一索引 idx_company_news_ticker_url 存在") # Avoid excessive logging
                except sqlite3.OperationalError as e:
                    print(f"创建唯一索引 idx_company_news_ticker_url 失败 (可能已存在或冲突): {e}")

                if updates_made:
                    self.conn.commit()
                    print("company_news 表结构已更新。")
                # else:
                #     print("company_news 表结构无需更新。") # Avoid excessive logging

        except sqlite3.Error as e:
            print(f"更新数据库表结构时出错: {e}")
            self.conn.rollback() # 出错时回滚
