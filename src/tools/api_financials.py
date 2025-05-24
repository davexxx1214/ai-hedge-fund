"""
财务指标相关API功能
"""
import pandas as pd
from datetime import datetime
import time

from src.tools.api_base import fd, check_rate_limit, calculate_growth, FIELD_MAPPING
from src.tools.api_cache import save_to_file_cache, should_refresh_financial_data, check_financial_cache_exists, get_target_date_for_financial_data, check_current_date_cache_exists, load_from_file_cache
from src.tools.api_models import MetricsWrapper
from src.data.db_cache import get_db_cache
from src.data.database_core import get_db
from src.data.cache import get_cache

# 内存缓存实例
cache = get_cache()

def get_financial_metrics(ticker: str, end_date: str = None, period: str = "ttm", limit: int = 10) -> list:
    """使用 Alpha Vantage 获取公司财务指标数据

    遵循缓存原则：
    1. 如果当前日期的JSON缓存存在，直接从DB获取数据
    2. 如果不存在，判断是否交易时间，优先请求上一个交易日的数据，然后保存到JSON缓存并更新DB
    
    返回的列表中包含一个支持 model_dump() 方法的 Metrics 对象。
    """
    # 获取数据库缓存实例
    db_cache = get_db_cache()
    db = get_db()
    
    # 构建缓存参数
    cache_params = {'end': end_date, 'period': period}
    
    # 检查当前日期的财务缓存是否存在
    if check_financial_cache_exists(ticker):
        print(f"发现 {ticker} 的当前日期财务缓存，从数据库获取数据")
        return _get_financial_metrics_from_db(ticker, db_cache, db)
    
    print(f"未发现 {ticker} 的当前日期财务缓存，从API获取数据")
    
    try:
        # 从API获取数据并保存
        return _fetch_and_save_financial_data(ticker, db_cache, cache_params)
    except Exception as e:
        print(f"Error fetching financial metrics for {ticker}: {str(e)}")
        return _get_default_metrics()

def _get_financial_metrics_from_db(ticker: str, db_cache, db) -> list:
    """从数据库获取财务指标"""
    try:
        # 从数据库获取各类财务数据
        income_stmt_annual = db.get_income_statement_annual(ticker)
        income_stmt_quarterly = db.get_income_statement_quarterly(ticker)
        balance_sheet_annual = db.get_balance_sheet_annual(ticker)
        balance_sheet_quarterly = db.get_balance_sheet_quarterly(ticker)
        cash_flow_annual = db.get_cash_flow_annual(ticker)
        cash_flow_quarterly = db.get_cash_flow_quarterly(ticker)
        
        if not income_stmt_annual:
            print(f"数据库中没有 {ticker} 的财务数据")
            return _get_default_metrics()
        
        # 转换为DataFrame格式以便计算
        income_stmt = pd.DataFrame(income_stmt_annual)
        balance_sheet = pd.DataFrame(balance_sheet_annual)
        cash_flow = pd.DataFrame(cash_flow_annual)
        
        # 尝试从缓存获取公司概览数据
        overview = _get_overview_from_cache_or_api(ticker)
        
        # 计算财务指标
        return _calculate_financial_metrics(ticker, income_stmt, balance_sheet, cash_flow, overview)
        
    except Exception as e:
        print(f"从数据库获取财务指标失败: {e}")
        return _get_default_metrics()

def _fetch_and_save_financial_data(ticker: str, db_cache, cache_params) -> list:
    """从API获取并保存财务数据"""
    try:
        # 获取公司概览数据
        overview = _get_overview_from_cache_or_api(ticker)
        print("DEBUG: Fetched overview")

        # 获取并保存利润表（年报）数据
        print("DEBUG: Fetching income statement annual...")
        check_rate_limit()
        income_stmt = _safe_get_income_statement_annual(ticker, db_cache, cache_params)
        print("DEBUG: Processed income statement annual")

        # 获取并保存利润表（季度）数据
        print("DEBUG: Fetching income statement quarterly...")
        check_rate_limit()
        income_stmt_quarterly = _safe_get_income_statement_quarterly(ticker, db_cache, cache_params)
        print("DEBUG: Processed income statement quarterly")

        # 获取并保存资产负债表（年报）数据
        print("DEBUG: Fetching balance sheet annual...")
        check_rate_limit()
        balance_sheet = _safe_get_balance_sheet_annual(ticker, db_cache, cache_params)
        print("DEBUG: Processed balance sheet annual")

        # 获取并保存资产负债表（季度）数据
        print("DEBUG: Fetching balance sheet quarterly...")
        check_rate_limit()
        balance_sheet_quarterly = _safe_get_balance_sheet_quarterly(ticker, db_cache, cache_params)
        print("DEBUG: Processed balance sheet quarterly")

        # 获取并保存现金流量表（年报）数据
        print("DEBUG: Fetching cash flow annual...")
        check_rate_limit()
        cash_flow = _safe_get_cash_flow_annual(ticker, db_cache, cache_params)
        print("DEBUG: Processed cash flow annual")

        # 获取并保存现金流量表（季度）数据
        print("DEBUG: Fetching cash flow quarterly...")
        check_rate_limit()
        cash_flow_quarterly = _safe_get_cash_flow_quarterly(ticker, db_cache, cache_params)
        print("DEBUG: Processed cash flow quarterly")
        
        # 计算财务指标
        return _calculate_financial_metrics(ticker, income_stmt, balance_sheet, cash_flow, overview)
        
    except Exception as e:
        print(f"Error fetching and saving financial data for {ticker}: {str(e)}")
        return _get_default_metrics()

def _get_overview_from_cache_or_api(ticker: str) -> pd.DataFrame:
    """优先从缓存获取公司概览数据，如果缓存不存在则调用API"""
    # 检查是否有当前日期的公司概览缓存
    if check_current_date_cache_exists('company_overview', ticker):
        print(f"从缓存获取 {ticker} 的公司概览数据")
        cached_overview = load_from_file_cache('company_overview', ticker)
        if cached_overview is not None:
            # 如果缓存数据是字典格式，转换为DataFrame
            if isinstance(cached_overview, list) and len(cached_overview) > 0:
                return pd.DataFrame([cached_overview[0]])
            elif isinstance(cached_overview, dict):
                return pd.DataFrame([cached_overview])
            else:
                return pd.DataFrame()
    
    # 如果缓存不存在，调用API并保存到缓存
    print(f"缓存不存在，从API获取 {ticker} 的公司概览数据")
    try:
        check_rate_limit()
        overview, _ = fd.get_company_overview(symbol=ticker)
        if not isinstance(overview, pd.DataFrame):
            overview = pd.DataFrame()
        
        # 保存到缓存
        if not overview.empty:
            overview_dict = overview.to_dict('records')
            save_to_file_cache('company_overview', ticker, overview_dict)
            print(f"已缓存 {ticker} 的公司概览数据")
        
        return overview
    except Exception as e:
        print(f"获取公司概览数据失败: {e}")
        return pd.DataFrame()

def _safe_get_income_statement_annual(ticker: str, db_cache, cache_params) -> pd.DataFrame:
    """安全获取年度利润表数据"""
    try:
        income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        if isinstance(income_stmt, list):
            print(f"Warning: fd.get_income_statement_annual returned a list, expected DataFrame. Resetting.")
            income_stmt = pd.DataFrame()
        elif not isinstance(income_stmt, pd.DataFrame):
            print(f"Warning: fd.get_income_statement_annual returned type {type(income_stmt)}, expected DataFrame. Resetting.")
            income_stmt = pd.DataFrame()
        
        # 保存数据
        if isinstance(income_stmt, pd.DataFrame) and len(income_stmt.index) > 0:
            try:
                income_stmt_dict = income_stmt.to_dict('records')
                db_cache.set_income_statement_annual(ticker, income_stmt_dict)
                save_to_file_cache('income_statement_annual', ticker, income_stmt_dict, cache_params)
                print(f"已保存 {ticker} 的利润表（年报）数据，共 {len(income_stmt)} 条记录")
            except Exception as e:
                print(f"Error saving income statement annual: {e}")
        
        return income_stmt
    except Exception as e:
        print(f"Error getting income statement annual: {str(e)}")
        return pd.DataFrame()

def _safe_get_income_statement_quarterly(ticker: str, db_cache, cache_params) -> pd.DataFrame:
    """安全获取季度利润表数据"""
    try:
        income_stmt_quarterly, _ = fd.get_income_statement_quarterly(symbol=ticker)
        if isinstance(income_stmt_quarterly, list):
            print(f"Warning: fd.get_income_statement_quarterly returned a list, expected DataFrame. Resetting.")
            income_stmt_quarterly = pd.DataFrame()
        elif not isinstance(income_stmt_quarterly, pd.DataFrame):
            print(f"Warning: fd.get_income_statement_quarterly returned type {type(income_stmt_quarterly)}, expected DataFrame. Resetting.")
            income_stmt_quarterly = pd.DataFrame()
        
        # 保存数据
        if isinstance(income_stmt_quarterly, pd.DataFrame) and len(income_stmt_quarterly.index) > 0:
            try:
                income_stmt_quarterly_dict = income_stmt_quarterly.to_dict('records')
                db_cache.set_income_statement_quarterly(ticker, income_stmt_quarterly_dict)
                save_to_file_cache('income_statement_quarterly', ticker, income_stmt_quarterly_dict, cache_params)
                print(f"已保存 {ticker} 的利润表（季度）数据，共 {len(income_stmt_quarterly)} 条记录")
            except Exception as e:
                print(f"Error saving income statement quarterly: {e}")
        
        return income_stmt_quarterly
    except Exception as e:
        print(f"Error getting quarterly income statement: {str(e)}")
        return pd.DataFrame()

def _safe_get_balance_sheet_annual(ticker: str, db_cache, cache_params) -> pd.DataFrame:
    """安全获取年度资产负债表数据"""
    try:
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        if isinstance(balance_sheet, list):
            print(f"Warning: fd.get_balance_sheet_annual returned a list, expected DataFrame. Resetting.")
            balance_sheet = pd.DataFrame()
        elif not isinstance(balance_sheet, pd.DataFrame):
            print(f"Warning: fd.get_balance_sheet_annual returned type {type(balance_sheet)}, expected DataFrame. Resetting.")
            balance_sheet = pd.DataFrame()
        
        # 保存数据
        if isinstance(balance_sheet, pd.DataFrame) and len(balance_sheet.index) > 0:
            try:
                balance_sheet_dict = balance_sheet.to_dict('records')
                db_cache.set_balance_sheet_annual(ticker, balance_sheet_dict)
                save_to_file_cache('balance_sheet_annual', ticker, balance_sheet_dict, cache_params)
                print(f"已保存 {ticker} 的资产负债表（年报）数据，共 {len(balance_sheet)} 条记录")
            except Exception as e:
                print(f"Error saving balance sheet annual: {e}")
        
        return balance_sheet
    except Exception as e:
        print(f"Error getting balance sheet: {str(e)}")
        return pd.DataFrame()

def _safe_get_balance_sheet_quarterly(ticker: str, db_cache, cache_params) -> pd.DataFrame:
    """安全获取季度资产负债表数据"""
    try:
        balance_sheet_quarterly, _ = fd.get_balance_sheet_quarterly(symbol=ticker)
        if isinstance(balance_sheet_quarterly, list):
            print(f"Warning: fd.get_balance_sheet_quarterly returned a list, expected DataFrame. Resetting.")
            balance_sheet_quarterly = pd.DataFrame()
        elif not isinstance(balance_sheet_quarterly, pd.DataFrame):
            print(f"Warning: fd.get_balance_sheet_quarterly returned type {type(balance_sheet_quarterly)}, expected DataFrame. Resetting.")
            balance_sheet_quarterly = pd.DataFrame()
        
        # 保存数据
        if isinstance(balance_sheet_quarterly, pd.DataFrame) and len(balance_sheet_quarterly.index) > 0:
            try:
                balance_sheet_quarterly_dict = balance_sheet_quarterly.to_dict('records')
                db_cache.set_balance_sheet_quarterly(ticker, balance_sheet_quarterly_dict)
                save_to_file_cache('balance_sheet_quarterly', ticker, balance_sheet_quarterly_dict, cache_params)
                print(f"已保存 {ticker} 的资产负债表（季度）数据，共 {len(balance_sheet_quarterly)} 条记录")
            except Exception as e:
                print(f"Error saving balance sheet quarterly: {e}")
        
        return balance_sheet_quarterly
    except Exception as e:
        print(f"Error getting quarterly balance sheet: {str(e)}")
        return pd.DataFrame()

def _safe_get_cash_flow_annual(ticker: str, db_cache, cache_params) -> pd.DataFrame:
    """安全获取年度现金流量表数据"""
    try:
        cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
        if isinstance(cash_flow, list):
            print(f"Warning: fd.get_cash_flow_annual returned a list, expected DataFrame. Resetting.")
            cash_flow = pd.DataFrame()
        elif not isinstance(cash_flow, pd.DataFrame):
            print(f"Warning: fd.get_cash_flow_annual returned type {type(cash_flow)}, expected DataFrame. Resetting.")
            cash_flow = pd.DataFrame()
        
        # 保存数据
        if isinstance(cash_flow, pd.DataFrame) and len(cash_flow.index) > 0:
            try:
                cash_flow_dict = cash_flow.to_dict('records')
                db_cache.set_cash_flow_annual(ticker, cash_flow_dict)
                save_to_file_cache('cash_flow_annual', ticker, cash_flow_dict, cache_params)
                print(f"已保存 {ticker} 的现金流量表（年报）数据，共 {len(cash_flow)} 条记录")
            except Exception as e:
                print(f"Error saving cash flow annual: {e}")
        
        return cash_flow
    except Exception as e:
        print(f"Error getting cash flow: {str(e)}")
        return pd.DataFrame()

def _safe_get_cash_flow_quarterly(ticker: str, db_cache, cache_params) -> pd.DataFrame:
    """安全获取季度现金流量表数据"""
    try:
        cash_flow_quarterly, _ = fd.get_cash_flow_quarterly(symbol=ticker)
        if isinstance(cash_flow_quarterly, list):
            print(f"Warning: fd.get_cash_flow_quarterly for {ticker} returned a list, expected DataFrame. Resetting.")
            cash_flow_quarterly = pd.DataFrame()
        elif not isinstance(cash_flow_quarterly, pd.DataFrame):
            print(f"Warning: fd.get_cash_flow_quarterly for {ticker} returned type {type(cash_flow_quarterly)}, expected DataFrame. Resetting.")
            cash_flow_quarterly = pd.DataFrame()
        
        if cash_flow_quarterly.empty:
            print(f"Info: fd.get_cash_flow_quarterly for {ticker} returned an empty DataFrame. This might be due to no data or an API issue not causing an exception.")

        # 保存数据
        if isinstance(cash_flow_quarterly, pd.DataFrame) and len(cash_flow_quarterly.index) > 0:
            try:
                cash_flow_quarterly_dict = cash_flow_quarterly.to_dict('records')
                db_cache.set_cash_flow_quarterly(ticker, cash_flow_quarterly_dict)
                save_to_file_cache('cash_flow_quarterly', ticker, cash_flow_quarterly_dict, cache_params)
                print(f"已保存 {ticker} 的现金流量表（季度）数据，共 {len(cash_flow_quarterly)} 条记录")
            except Exception as e:
                print(f"Error saving cash flow quarterly: {e}")
        
        return cash_flow_quarterly
    except Exception as e:
        print(f"CRITICAL_API_ERROR: Error calling fd.get_cash_flow_quarterly for {ticker}: {str(e)}. Will proceed with empty quarterly cash flow data.")
        return pd.DataFrame()

def _calculate_financial_metrics(ticker: str, income_stmt: pd.DataFrame, balance_sheet: pd.DataFrame, 
                                cash_flow: pd.DataFrame, overview: pd.DataFrame) -> list:
    """计算财务指标"""
    try:
        operating_cash = 0
        capex = 0
        shares = 0
        
        if "operatingCashflow" in cash_flow.columns and len(cash_flow.index) > 0:
            operating_cash = float(cash_flow["operatingCashflow"].iloc[0])
        
        if "capitalExpenditures" in cash_flow.columns and len(cash_flow.index) > 0:
            capex = float(cash_flow["capitalExpenditures"].iloc[0])
        
        if "SharesOutstanding" in overview.columns and len(overview.index) > 0:
            shares = float(overview["SharesOutstanding"].iloc[0])
        
        free_cash_flow = operating_cash - capex
        free_cash_flow_per_share = free_cash_flow / shares if shares != 0 else 0
        
        # 计算股权发行与回购净额
        issuance_or_purchase_of_equity_shares = 0
        if len(cash_flow.index) > 0:
            # 计算股权发行净额
            equity_issuance = 0
            if "proceedsFromIssuanceOfCommonStock" in cash_flow.columns:
                common_stock_issuance = cash_flow["proceedsFromIssuanceOfCommonStock"].iloc[0]
                if common_stock_issuance and common_stock_issuance != 'None':
                    equity_issuance += float(common_stock_issuance)
            if "proceedsFromIssuanceOfPreferredStock" in cash_flow.columns:
                preferred_stock_issuance = cash_flow["proceedsFromIssuanceOfPreferredStock"].iloc[0]
                if preferred_stock_issuance and preferred_stock_issuance != 'None':
                    equity_issuance += float(preferred_stock_issuance)
            
            # 计算股权回购净额
            equity_repurchase = 0
            if "paymentsForRepurchaseOfCommonStock" in cash_flow.columns:
                common_stock_repurchase = cash_flow["paymentsForRepurchaseOfCommonStock"].iloc[0]
                if common_stock_repurchase and common_stock_repurchase != 'None':
                    equity_repurchase += float(common_stock_repurchase)
            if "paymentsForRepurchaseOfEquity" in cash_flow.columns:
                equity_repurchase_general = cash_flow["paymentsForRepurchaseOfEquity"].iloc[0]
                if equity_repurchase_general and equity_repurchase_general != 'None':
                    equity_repurchase += float(equity_repurchase_general)
            if "paymentsForRepurchaseOfPreferredStock" in cash_flow.columns:
                preferred_stock_repurchase = cash_flow["paymentsForRepurchaseOfPreferredStock"].iloc[0]
                if preferred_stock_repurchase and preferred_stock_repurchase != 'None':
                    equity_repurchase += float(preferred_stock_repurchase)
            
            # 计算净额
            issuance_or_purchase_of_equity_shares = equity_issuance - equity_repurchase
    except Exception as e:
        print(f"Error calculating equity issuance/repurchase: {str(e)}")
        issuance_or_purchase_of_equity_shares = 0
    
    # 获取财报日期
    report_date = None
    if "fiscalDateEnding" in income_stmt.columns and len(income_stmt.index) > 0:
        report_date = income_stmt["fiscalDateEnding"].iloc[0]
    
    # 计算财务指标 - 如果数据不存在，显示为None而不是0
    def safe_get_float(df, column, default=None):
        """安全获取数值，如果列不存在或为空则返回default"""
        if column in df.columns and len(df.index) > 0:
            value = df[column].iloc[0]
            try:
                return float(value) if value is not None and str(value) != 'None' else default
            except (ValueError, TypeError):
                return default
        return default
    
    # 计算book_value_growth (从totalShareholderEquity计算)
    book_value_growth = None
    if "totalShareholderEquity" in balance_sheet.columns and len(balance_sheet.index) >= 2:
        try:
            current_equity = float(balance_sheet["totalShareholderEquity"].iloc[0])
            previous_equity = float(balance_sheet["totalShareholderEquity"].iloc[1])
            if previous_equity != 0:
                book_value_growth = (current_equity - previous_equity) / previous_equity
        except (ValueError, TypeError, IndexError):
            pass
    
    # 计算current_ratio (Current Assets / Current Liabilities)
    current_ratio = None
    if ("totalCurrentAssets" in balance_sheet.columns and 
        "totalCurrentLiabilities" in balance_sheet.columns and len(balance_sheet.index) > 0):
        try:
            current_assets = float(balance_sheet["totalCurrentAssets"].iloc[0])
            current_liabilities = float(balance_sheet["totalCurrentLiabilities"].iloc[0])
            if current_liabilities != 0:
                current_ratio = current_assets / current_liabilities
        except (ValueError, TypeError):
            pass
    
    # 计算debt_to_equity (Total Debt / Total Equity)
    debt_to_equity = None
    if "totalShareholderEquity" in balance_sheet.columns and len(balance_sheet.index) > 0:
        try:
            total_equity = float(balance_sheet["totalShareholderEquity"].iloc[0])
            # 计算总债务 = 短期债务 + 长期债务
            total_debt = 0
            if "shortTermDebt" in balance_sheet.columns:
                short_debt = balance_sheet["shortTermDebt"].iloc[0]
                if short_debt and short_debt != 'None':
                    total_debt += float(short_debt)
            if "longTermDebt" in balance_sheet.columns:
                long_debt = balance_sheet["longTermDebt"].iloc[0]
                if long_debt and long_debt != 'None':
                    total_debt += float(long_debt)
            
            if total_equity != 0:
                debt_to_equity = total_debt / total_equity
        except (ValueError, TypeError):
            pass
    
    # 计算enterprise_value (Market Cap + Total Debt - Cash)
    enterprise_value = None
    market_cap = safe_get_float(overview, "MarketCapitalization")
    if market_cap and "totalShareholderEquity" in balance_sheet.columns and len(balance_sheet.index) > 0:
        try:
            # 计算总债务
            total_debt = 0
            if "shortTermDebt" in balance_sheet.columns:
                short_debt = balance_sheet["shortTermDebt"].iloc[0]
                if short_debt and short_debt != 'None':
                    total_debt += float(short_debt)
            if "longTermDebt" in balance_sheet.columns:
                long_debt = balance_sheet["longTermDebt"].iloc[0]
                if long_debt and long_debt != 'None':
                    total_debt += float(long_debt)
            
            # 获取现金和现金等价物
            cash = 0
            if "cashAndCashEquivalentsAtCarryingValue" in balance_sheet.columns:
                cash_value = balance_sheet["cashAndCashEquivalentsAtCarryingValue"].iloc[0]
                if cash_value and cash_value != 'None':
                    cash = float(cash_value)
            elif "cashAndShortTermInvestments" in balance_sheet.columns:
                cash_value = balance_sheet["cashAndShortTermInvestments"].iloc[0]
                if cash_value and cash_value != 'None':
                    cash = float(cash_value)
            
            enterprise_value = market_cap + total_debt - cash
        except (ValueError, TypeError):
            pass

    metrics_data = {
        "return_on_equity": safe_get_float(overview, "ReturnOnEquityTTM"),
        "net_margin": safe_get_float(overview, "ProfitMargin"),
        "operating_margin": safe_get_float(overview, "OperatingMarginTTM"),
        "revenue_growth": calculate_growth(income_stmt, "totalRevenue") if "totalRevenue" in income_stmt.columns and len(income_stmt.index) > 0 else None,
        "earnings_growth": calculate_growth(income_stmt, "netIncome") if "netIncome" in income_stmt.columns and len(income_stmt.index) > 0 else None,
        "book_value_growth": book_value_growth,
        "current_ratio": current_ratio,
        "debt_to_equity": debt_to_equity,
        "price_to_earnings_ratio": safe_get_float(overview, "PERatio"),
        "price_to_book_ratio": safe_get_float(overview, "PriceToBookRatio"),
        "price_to_sales_ratio": safe_get_float(overview, "PriceToSalesRatioTTM"),
        "earnings_per_share": safe_get_float(overview, "EPS"),
        "free_cash_flow_per_share": free_cash_flow_per_share if shares != 0 else None,
        "issuance_or_purchase_of_equity_shares": issuance_or_purchase_of_equity_shares,
        "enterprise_value": enterprise_value,
        "enterprise_value_to_ebitda_ratio": safe_get_float(overview, "EVToEBITDA"),
        "market_cap": safe_get_float(overview, "MarketCapitalization"),
        "report_period": report_date or datetime.now().strftime('%Y-%m-%d')
    }
    
    # 清理None值，对于确实需要显示缺失数据的情况
    missing_fields = [k for k, v in metrics_data.items() if v is None]
    if missing_fields:
        print(f"Warning: {ticker} 缺失以下财务指标: {', '.join(missing_fields)}")
    
    metrics = MetricsWrapper(metrics_data)
    return [metrics]

def _get_default_metrics() -> list:
    """获取默认的空指标"""
    default_data = {
        "return_on_equity": None,
        "net_margin": None,
        "operating_margin": None,
        "revenue_growth": None,
        "earnings_growth": None,
        "book_value_growth": None,
        "current_ratio": None,
        "debt_to_equity": None,
        "price_to_earnings_ratio": None,
        "price_to_book_ratio": None,
        "price_to_sales_ratio": None,
        "earnings_per_share": None,
        "free_cash_flow_per_share": None,
        "issuance_or_purchase_of_equity_shares": None,
        "enterprise_value": None,
        "enterprise_value_to_ebitda_ratio": None,
        "market_cap": None,
        "report_period": datetime.now().strftime('%Y-%m-%d')
    }
    return [MetricsWrapper(default_data)]

def search_line_items(ticker: str, line_items: list, end_date: str = None, period: str = "ttm", limit: int = 2) -> list:
    """使用 Alpha Vantage 获取指定财报项目

    遵循缓存原则：
    1. 如果当前日期的JSON缓存存在，直接从DB获取数据
    2. 如果不存在，判断是否交易时间，优先请求上一个交易日的数据，然后保存到JSON缓存并更新DB
    
    函数从年报数据中抽取所需的项目（如自由现金流、净利润、收入、经营利润率等），
    返回包含属性访问的 FinancialData 对象列表。
    """
    # 获取数据库缓存实例
    db_cache = get_db_cache()
    db = get_db()
    
    # 构建缓存参数
    cache_params = {'end': end_date, 'period': period, 'items': '_'.join(line_items)}
    
    # 检查当前日期的财务缓存是否存在
    if check_financial_cache_exists(ticker):
        print(f"发现 {ticker} 的当前日期财务缓存，从数据库获取line items数据")
        return _get_line_items_from_db(ticker, line_items, limit, db)
    
    print(f"未发现 {ticker} 的当前日期财务缓存，从API获取line items数据")
    
    # 从API获取数据并保存
    try:
        return _fetch_and_get_line_items(ticker, line_items, limit, cache_params)
    except Exception as e:
        print(f"Error fetching line items for {ticker}: {str(e)}")
        return []

def _get_line_items_from_db(ticker: str, line_items: list, limit: int, db) -> list:
    """从数据库获取指定的财报项目"""
    try:
        # 从数据库获取各类财务数据
        income_stmt_annual = db.get_income_statement_annual(ticker)
        balance_sheet_annual = db.get_balance_sheet_annual(ticker)
        cash_flow_annual = db.get_cash_flow_annual(ticker)
        
        if not income_stmt_annual:
            print(f"数据库中没有 {ticker} 的财务数据")
            return []
        
        # 转换为DataFrame格式以便处理
        income_stmt = pd.DataFrame(income_stmt_annual)
        balance_sheet = pd.DataFrame(balance_sheet_annual)
        cash_flow = pd.DataFrame(cash_flow_annual)
        
        # 尝试从缓存获取公司概览数据
        overview = _get_overview_from_cache_or_api(ticker)
        
        # 计算line items
        return _calculate_line_items(ticker, line_items, limit, income_stmt, balance_sheet, cash_flow, overview)
        
    except Exception as e:
        print(f"从数据库获取line items失败: {e}")
        return []

def _fetch_and_get_line_items(ticker: str, line_items: list, limit: int, cache_params) -> list:
    """从API获取财务数据并计算line items"""
    try:
        # 检查 API 请求限制
        check_rate_limit()
        income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        
        check_rate_limit()
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        
        check_rate_limit()
        cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
        
        # 获取公司概览数据
        overview = _get_overview_from_cache_or_api(ticker)

        # 保存到数据库和缓存（如果数据有效）
        db_cache = get_db_cache()
        
        if isinstance(income_stmt, pd.DataFrame) and len(income_stmt.index) > 0:
            income_stmt_dict = income_stmt.to_dict('records')
            db_cache.set_income_statement_annual(ticker, income_stmt_dict)
            save_to_file_cache('income_statement_annual', ticker, income_stmt_dict, cache_params)
        
        if isinstance(balance_sheet, pd.DataFrame) and len(balance_sheet.index) > 0:
            balance_sheet_dict = balance_sheet.to_dict('records')
            db_cache.set_balance_sheet_annual(ticker, balance_sheet_dict)
            save_to_file_cache('balance_sheet_annual', ticker, balance_sheet_dict, cache_params)
        
        if isinstance(cash_flow, pd.DataFrame) and len(cash_flow.index) > 0:
            cash_flow_dict = cash_flow.to_dict('records')
            db_cache.set_cash_flow_annual(ticker, cash_flow_dict)
            save_to_file_cache('cash_flow_annual', ticker, cash_flow_dict, cache_params)

        # 计算line items
        return _calculate_line_items(ticker, line_items, limit, income_stmt, balance_sheet, cash_flow, overview)
        
    except Exception as e:
        print(f"Error fetching and calculating line items for {ticker}: {str(e)}")
        return []

def _calculate_line_items(ticker: str, line_items: list, limit: int, income_stmt: pd.DataFrame, 
                         balance_sheet: pd.DataFrame, cash_flow: pd.DataFrame, overview: pd.DataFrame) -> list:
    """计算指定的财报项目"""
    results = []
    
    # 确保数据是DataFrame格式
    if not isinstance(income_stmt, pd.DataFrame):
        income_stmt = pd.DataFrame()
    if not isinstance(balance_sheet, pd.DataFrame):
        balance_sheet = pd.DataFrame()
    if not isinstance(cash_flow, pd.DataFrame):
        cash_flow = pd.DataFrame()
    if not isinstance(overview, pd.DataFrame):
        overview = pd.DataFrame()
    
    max_records = min(limit, len(income_stmt)) if len(income_stmt) > 0 else limit
    
    for i in range(max_records):
        data = {}
        
        # 获取财报日期
        if "fiscalDateEnding" in income_stmt.columns.tolist() and i < len(income_stmt):
            data["report_period"] = income_stmt["fiscalDateEnding"].iloc[i]
        else:
            data["report_period"] = datetime.now().strftime('%Y-%m-%d')
            
        for item in line_items:
            # 使用原有的完整逻辑处理每个item
            mapped_item = FIELD_MAPPING.get(item, item)
            
            # 尝试从各个数据源获取数据
            value = None
            found = False
            
            # 从income_stmt获取
            if not found and mapped_item in income_stmt.columns.tolist() and i < len(income_stmt):
                try:
                    value = float(income_stmt[mapped_item].iloc[i])
                    found = True
                except (ValueError, TypeError):
                    pass
            
            # 从balance_sheet获取
            if not found and mapped_item in balance_sheet.columns.tolist() and i < len(balance_sheet):
                try:
                    value = float(balance_sheet[mapped_item].iloc[i])
                    found = True
                except (ValueError, TypeError):
                    pass
            
            # 从cash_flow获取
            if not found and mapped_item in cash_flow.columns.tolist() and i < len(cash_flow):
                try:
                    value = float(cash_flow[mapped_item].iloc[i])
                    found = True
                except (ValueError, TypeError):
                    pass
            
            # 从overview获取
            if not found and mapped_item in overview.columns.tolist() and len(overview) > 0:
                try:
                    value = float(overview[mapped_item].iloc[0])
                    found = True
                except (ValueError, TypeError):
                    pass
            
            # 特殊计算项目
            if not found:
                if item == "free_cash_flow":
                    try:
                        if ("operatingCashflow" in cash_flow.columns.tolist() and 
                            "capitalExpenditures" in cash_flow.columns.tolist() and i < len(cash_flow)):
                            operating_cash = float(cash_flow["operatingCashflow"].iloc[i])
                            capex = float(cash_flow["capitalExpenditures"].iloc[i])
                            value = operating_cash - capex
                            found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "working_capital":
                    try:
                        # Working Capital = Current Assets - Current Liabilities
                        if ("totalCurrentAssets" in balance_sheet.columns.tolist() and 
                            "totalCurrentLiabilities" in balance_sheet.columns.tolist() and i < len(balance_sheet)):
                            current_assets = float(balance_sheet["totalCurrentAssets"].iloc[i])
                            current_liabilities = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                            value = current_assets - current_liabilities
                            found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "depreciation_and_amortization":
                    try:
                        # 从现金流量表获取折旧和摊销
                        if "depreciationAndAmortization" in cash_flow.columns.tolist() and i < len(cash_flow):
                            value = float(cash_flow["depreciationAndAmortization"].iloc[i])
                            found = True
                        elif "depreciation" in cash_flow.columns.tolist() and i < len(cash_flow):
                            value = float(cash_flow["depreciation"].iloc[i])
                            found = True
                        elif "depreciationDepletionAndAmortization" in cash_flow.columns.tolist() and i < len(cash_flow):
                            value = float(cash_flow["depreciationDepletionAndAmortization"].iloc[i])
                            found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "capital_expenditure":
                    try:
                        # 资本支出通常在现金流量表中
                        if "capitalExpenditures" in cash_flow.columns.tolist() and i < len(cash_flow):
                            value = float(cash_flow["capitalExpenditures"].iloc[i])
                            # 通常资本支出是负数，但我们需要正数用于计算
                            value = abs(value)
                            found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "earnings_per_share":
                    try:
                        if "netIncome" in income_stmt.columns.tolist() and i < len(income_stmt):
                            net_income = float(income_stmt["netIncome"].iloc[i])
                            shares = 0
                            if ("commonStockSharesOutstanding" in balance_sheet.columns.tolist() and 
                                i < len(balance_sheet)):
                                shares = float(balance_sheet["commonStockSharesOutstanding"].iloc[i])
                            elif "SharesOutstanding" in overview.columns.tolist() and len(overview) > 0:
                                shares = float(overview["SharesOutstanding"].iloc[0])
                            if shares != 0:
                                value = net_income / shares
                                found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "issuance_or_purchase_of_equity_shares":
                    try:
                        if i < len(cash_flow):
                            # 计算股权发行净额
                            equity_issuance = 0
                            if "proceedsFromIssuanceOfCommonStock" in cash_flow.columns.tolist():
                                common_stock_issuance = cash_flow["proceedsFromIssuanceOfCommonStock"].iloc[i]
                                if common_stock_issuance and common_stock_issuance != 'None':
                                    equity_issuance += float(common_stock_issuance)
                            if "proceedsFromIssuanceOfPreferredStock" in cash_flow.columns.tolist():
                                preferred_stock_issuance = cash_flow["proceedsFromIssuanceOfPreferredStock"].iloc[i]
                                if preferred_stock_issuance and preferred_stock_issuance != 'None':
                                    equity_issuance += float(preferred_stock_issuance)
                            
                            # 计算股权回购净额
                            equity_repurchase = 0
                            if "paymentsForRepurchaseOfCommonStock" in cash_flow.columns.tolist():
                                common_stock_repurchase = cash_flow["paymentsForRepurchaseOfCommonStock"].iloc[i]
                                if common_stock_repurchase and common_stock_repurchase != 'None':
                                    equity_repurchase += float(common_stock_repurchase)
                            if "paymentsForRepurchaseOfEquity" in cash_flow.columns.tolist():
                                equity_repurchase_general = cash_flow["paymentsForRepurchaseOfEquity"].iloc[i]
                                if equity_repurchase_general and equity_repurchase_general != 'None':
                                    equity_repurchase += float(equity_repurchase_general)
                            if "paymentsForRepurchaseOfPreferredStock" in cash_flow.columns.tolist():
                                preferred_stock_repurchase = cash_flow["paymentsForRepurchaseOfPreferredStock"].iloc[i]
                                if preferred_stock_repurchase and preferred_stock_repurchase != 'None':
                                    equity_repurchase += float(preferred_stock_repurchase)
                            
                            # 计算净额
                            value = equity_issuance - equity_repurchase
                            found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "gross_margin":
                    try:
                        # Gross Margin = Gross Profit / Total Revenue
                        if ("grossProfit" in income_stmt.columns.tolist() and 
                            "totalRevenue" in income_stmt.columns.tolist() and i < len(income_stmt)):
                            gross_profit = float(income_stmt["grossProfit"].iloc[i])
                            total_revenue = float(income_stmt["totalRevenue"].iloc[i])
                            if total_revenue != 0:
                                value = gross_profit / total_revenue
                                found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "operating_margin":
                    try:
                        # Operating Margin = Operating Income / Total Revenue
                        if ("operatingIncome" in income_stmt.columns.tolist() and 
                            "totalRevenue" in income_stmt.columns.tolist() and i < len(income_stmt)):
                            operating_income = float(income_stmt["operatingIncome"].iloc[i])
                            total_revenue = float(income_stmt["totalRevenue"].iloc[i])
                            if total_revenue != 0:
                                value = operating_income / total_revenue
                                found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "research_and_development":
                    try:
                        # 研发费用直接从利润表获取
                        if "researchAndDevelopment" in income_stmt.columns.tolist() and i < len(income_stmt):
                            value = float(income_stmt["researchAndDevelopment"].iloc[i])
                            found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "total_debt":
                    try:
                        # Total Debt = Short Term Debt + Long Term Debt
                        total_debt = 0
                        debt_found = False
                        
                        if i < len(balance_sheet):
                            # 短期债务
                            if "shortTermDebt" in balance_sheet.columns.tolist():
                                short_term_debt = balance_sheet["shortTermDebt"].iloc[i]
                                if short_term_debt and short_term_debt != 'None':
                                    total_debt += float(short_term_debt)
                                    debt_found = True
                            
                            # 长期债务
                            if "longTermDebt" in balance_sheet.columns.tolist():
                                long_term_debt = balance_sheet["longTermDebt"].iloc[i]
                                if long_term_debt and long_term_debt != 'None':
                                    total_debt += float(long_term_debt)
                                    debt_found = True
                            
                            # 总债务（如果直接可用）
                            if "totalDebt" in balance_sheet.columns.tolist():
                                total_debt_direct = balance_sheet["totalDebt"].iloc[i]
                                if total_debt_direct and total_debt_direct != 'None':
                                    total_debt = float(total_debt_direct)
                                    debt_found = True
                        
                        if debt_found:
                            value = total_debt
                            found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "goodwill_and_intangible_assets":
                    try:
                        # 商誉和无形资产
                        if i < len(balance_sheet):
                            goodwill_total = 0
                            goodwill_found = False
                            
                            if "goodwill" in balance_sheet.columns.tolist():
                                goodwill = balance_sheet["goodwill"].iloc[i]
                                if goodwill and goodwill != 'None':
                                    goodwill_total += float(goodwill)
                                    goodwill_found = True
                            
                            if "intangibleAssets" in balance_sheet.columns.tolist():
                                intangible = balance_sheet["intangibleAssets"].iloc[i]
                                if intangible and intangible != 'None':
                                    goodwill_total += float(intangible)
                                    goodwill_found = True
                            
                            if "goodwillAndIntangibleAssets" in balance_sheet.columns.tolist():
                                goodwill_intangible = balance_sheet["goodwillAndIntangibleAssets"].iloc[i]
                                if goodwill_intangible and goodwill_intangible != 'None':
                                    goodwill_total = float(goodwill_intangible)
                                    goodwill_found = True
                            
                            if goodwill_found:
                                value = goodwill_total
                                found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                
                elif item == "return_on_invested_capital":
                    try:
                        # ROIC = NOPAT / Invested Capital (简化计算)
                        if ("operatingIncome" in income_stmt.columns.tolist() and 
                            "totalAssets" in balance_sheet.columns.tolist() and 
                            i < len(income_stmt) and i < len(balance_sheet)):
                            operating_income = float(income_stmt["operatingIncome"].iloc[i])
                            total_assets = float(balance_sheet["totalAssets"].iloc[i])
                            if total_assets != 0:
                                # 简化的ROIC计算
                                value = operating_income / total_assets
                                found = True
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
            
            # 设置值，如果未找到则设为None（不使用默认值0）
            if found:
                data[item] = value
            else:
                print(f"Warning: 无法找到 {ticker} 的 {item} 数据")
                data[item] = None
        
        results.append(MetricsWrapper(data))
    
    return results

def check_and_update_financials(ticker: str):
    """检查缓存是否需要更新，必要时下载并保存最新财务数据"""
    try:
        if should_refresh_financial_data(ticker):
            print(f"检测到 {ticker} 财报需要更新，开始下载...")
            get_financial_metrics(ticker)
            print(f"{ticker} 财报已更新完成。")
        else:
            print(f"{ticker} 财报缓存有效，无需更新。")
    except Exception as e:
        print(f"检查更新财报失败: {e}")

def get_income_statement(ticker: str, period: str = "annual"):
    """获取利润表数据"""
    check_rate_limit()
    
    try:
        if period.lower() == "annual":
            income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        else:
            income_stmt, _ = fd.get_income_statement_quarterly(symbol=ticker)
        
        if len(income_stmt.index) == 0:
            print(f"没有找到 {ticker} 的利润表数据")
            return []
        
        # 转换为字典列表
        return income_stmt.to_dict('records')
    except Exception as e:
        print(f"获取 {ticker} 的利润表数据时出错: {str(e)}")
        return []

def get_balance_sheet(ticker: str, period: str = "annual"):
    """获取资产负债表数据"""
    check_rate_limit()
    
    try:
        if period.lower() == "annual":
            balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        else:
            balance_sheet, _ = fd.get_balance_sheet_quarterly(symbol=ticker)
        
        if len(balance_sheet.index) == 0:
            print(f"没有找到 {ticker} 的资产负债表数据")
            return []
        
        # 转换为字典列表
        return balance_sheet.to_dict('records')
    except Exception as e:
        print(f"获取 {ticker} 的资产负债表数据时出错: {str(e)}")
        return []

def get_cash_flow(ticker: str, period: str = "annual"):
    """获取现金流量表数据"""
    check_rate_limit()
    
    try:
        if period.lower() == "annual":
            cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
        else:
            cash_flow, _ = fd.get_cash_flow_quarterly(symbol=ticker)
        
        if len(cash_flow.index) == 0:
            print(f"没有找到 {ticker} 的现金流量表数据")
            return []
        
        # 转换为字典列表
        return cash_flow.to_dict('records')
    except Exception as e:
        print(f"获取 {ticker} 的现金流量表数据时出错: {str(e)}")
        return [] 