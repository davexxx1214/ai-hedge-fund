from dotenv import load_dotenv
load_dotenv()

import os
import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData

# 获取缓存实例
from src.data.cache import get_cache

# 创建本地文件缓存目录
CACHE_DIR = Path("src/data/cache_files")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 财报发布时间规律（美国公司）
# Q1（1-3月）财报：4月中旬至5月初发布
# Q2（4-6月）财报：7月中旬至8月初发布
# Q3（7-9月）财报：10月中旬至11月初发布
# Q4/年报（10-12月）财报：次年1月中旬至2月底发布

# API 请求限制控制
REQUEST_LIMIT = 75  # 每分钟最大请求数
request_timestamps = []  # 记录请求时间戳

# 从环境变量中获取 ALPHAVANTAGE API Key
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
# 初始化 Alpha Vantage 客户端
ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format="pandas")
fd = FundamentalData(key=ALPHA_VANTAGE_API_KEY, output_format="pandas")

# 内存缓存实例
cache = get_cache()

def check_rate_limit():
    """检查 API 请求频率限制，必要时等待"""
    global request_timestamps
    
    current_time = time.time()
    # 清理超过一分钟的时间戳
    request_timestamps = [ts for ts in request_timestamps if current_time - ts < 60]
    
    # 如果已达到限制，等待
    if len(request_timestamps) >= REQUEST_LIMIT:
        oldest_timestamp = min(request_timestamps)
        sleep_time = 60 - (current_time - oldest_timestamp) + 0.1  # 额外等待 0.1 秒以确保安全
        if sleep_time > 0:
            print(f"API 请求达到限制，等待 {sleep_time:.2f} 秒...")
            time.sleep(sleep_time)
            # 重新检查（递归调用）
            return check_rate_limit()
    
    # 记录新的请求时间戳
    request_timestamps.append(time.time())

def get_cache_path(cache_type, ticker, params=None):
    """获取缓存文件路径"""
    if params:
        # 将参数转换为字符串用于文件名
        params_str = "_".join(f"{k}_{v}" for k, v in sorted(params.items()) if v is not None)
        filename = f"{ticker}_{params_str}.json"
    else:
        filename = f"{ticker}.json"
    
    return CACHE_DIR / cache_type / filename

def save_to_file_cache(cache_type, ticker, data, params=None):
    """保存数据到文件缓存"""
    cache_path = get_cache_path(cache_type, ticker, params)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # 对于不同类型的数据，可能需要不同的序列化方法
        if isinstance(data, list) and len(data) > 0:
            if hasattr(data[0], 'model_dump'):
                # 如果是对象列表，使用 model_dump 方法
                serialized_data = [item.model_dump() if hasattr(item, 'model_dump') else item for item in data]
            elif cache_type == 'insider_trades':
                # 特殊处理 insider_trades 数据
                serialized_data = []
                for item in data:
                    # 将对象的属性转换为字典
                    item_dict = {}
                    for attr in dir(item):
                        # 跳过私有属性和方法
                        if not attr.startswith('_') and not callable(getattr(item, attr)):
                            item_dict[attr] = getattr(item, attr)
                    serialized_data.append(item_dict)
            else:
                # 其他情况直接保存
                serialized_data = data
        else:
            # 其他情况直接保存
            serialized_data = data
        
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(serialized_data, f, ensure_ascii=False, default=str)
        
        print(f"已缓存 {ticker} 的 {cache_type} 数据到 {cache_path}")
        return True
    except Exception as e:
        print(f"缓存 {ticker} 的 {cache_type} 数据失败: {str(e)}")
        return False

def load_from_file_cache(cache_type, ticker, params=None, max_age_days=30):
    """从文件缓存加载数据"""
    cache_path = get_cache_path(cache_type, ticker, params)
    
    if not cache_path.exists():
        return None
    
    # 检查缓存文件是否过期
    file_age = (datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)).days
    if file_age > max_age_days:
        print(f"缓存文件 {cache_path} 已过期 ({file_age} 天)")
        return None
    
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 根据缓存类型处理数据
        if cache_type == 'financial_metrics':
            return [MetricsWrapper(item) for item in data]
        elif cache_type == 'line_items':
            return [MetricsWrapper(item) for item in data]
        elif cache_type == 'insider_trades':
            # 创建具有属性访问的对象
            return [type('InsiderTrade', (), item if isinstance(item, dict) else {'error': 'Invalid data format'})() for item in data]
        elif cache_type == 'company_news':
            # 创建 CompanyNews 对象
            return [CompanyNews(**item) for item in data]
        else:
            return data
    except Exception as e:
        print(f"加载 {ticker} 的 {cache_type} 缓存数据失败: {str(e)}")
        return None

def should_refresh_financial_data(ticker, end_date=None):
    """判断是否应该刷新财务数据（基于财报发布时间规律）"""
    if end_date:
        # 如果提供了特定日期，检查该日期是否在财报发布期间
        target_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        target_date = datetime.now()
    
    month = target_date.month
    day = target_date.day
    
    # 判断是否在财报发布期
    if (month == 4 and day >= 15) or (month == 5 and day <= 10):  # Q1财报期
        return True
    elif (month == 7 and day >= 15) or (month == 8 and day <= 10):  # Q2财报期
        return True
    elif (month == 10 and day >= 15) or (month == 11 and day <= 10):  # Q3财报期
        return True
    elif (month == 1 and day >= 15) or (month == 2):  # Q4/年报财报期
        return True
    
    # 检查缓存文件是否存在
    cache_path = get_cache_path('financial_metrics', ticker)
    if not cache_path.parent.exists() or not cache_path.exists():
        return True  # 如果缓存不存在，需要刷新
    
    return False  # 其他情况不需要刷新

class MetricsWrapper:
    """
    用于包装财务指标数据，使之支持 model_dump() 方法（类似 pydantic 对象）
    """
    def __init__(self, data: dict):
        self.__dict__.update(data)
    def model_dump(self):
        return self.__dict__

def get_prices(ticker: str, start_date: str, end_date: str = None) -> list:
    """使用 Alpha Vantage 获取历史价格数据

    返回的数据列表中，每个记录包含字段：
    time, open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient
    注意：这里将原来的"date"字段改为"time"，以便与后续转换保持一致。
    """
    # 构建缓存参数
    cache_params = {'start': start_date, 'end': end_date}
    
    # 尝试从内存缓存获取
    cached_data = cache.get_prices(ticker)
    if cached_data:
        # 过滤日期范围
        filtered_data = [item for item in cached_data if item['time'] >= start_date and (not end_date or item['time'] <= end_date)]
        if filtered_data:
            print(f"从内存缓存获取 {ticker} 的价格数据")
            return filtered_data
    
    # 尝试从文件缓存获取
    file_cached_data = load_from_file_cache('prices', ticker, cache_params)
    if file_cached_data:
        # 更新内存缓存
        if isinstance(file_cached_data, list) and len(file_cached_data) > 0:
            cache.set_prices(ticker, file_cached_data)
        print(f"从文件缓存获取 {ticker} 的价格数据")
        return file_cached_data
    
    # 如果缓存中没有，则从 API 获取
    try:
        # 检查 API 请求限制
        check_rate_limit()
        
        data, meta_data = ts.get_daily_adjusted(symbol=ticker, outputsize="full")
        data = data.reset_index()
        # 重命名列，使得第一列为 time（日期）
        data.columns = ['time', 'open', 'high', 'low', 'close', 'adjusted_close', 
                        'volume', 'dividend_amount', 'split_coefficient']
        # 将日期转换为字符串格式（如 'YYYY-MM-DD'）
        data['time'] = data['time'].dt.strftime('%Y-%m-%d')
        # 过滤指定的日期范围
        mask = (data['time'] >= start_date)
        if end_date:
            mask &= (data['time'] <= end_date)
        filtered_data = data.loc[mask]
        result = filtered_data.to_dict('records')
        
        # 保存到缓存
        cache.set_prices(ticker, result)
        save_to_file_cache('prices', ticker, result, cache_params)
        
        return result
    except Exception as e:
        print(f"Error fetching price data for {ticker}: {str(e)}")
        return []
    
def calculate_growth(df: pd.DataFrame, column_name: str) -> float:
    """计算增长率, 用于收入、净利润、股东权益等数据"""
    try:
        if column_name not in df.columns or len(df) < 2:
            return 0
        current = float(df[column_name].iloc[0])
        previous = float(df[column_name].iloc[1])
        return (current - previous) / previous if previous != 0 else 0
    except Exception as e:
        print(f"Error calculating growth for {column_name}: {str(e)}")
        return 0

def get_financial_metrics(ticker: str, end_date: str = None, period: str = "ttm", limit: int = 10) -> list:
    """使用 Alpha Vantage 获取公司财务指标数据

    通过 FundamentalData 接口获取公司概览、年报数据，并计算各项财务比率和增长率。
    返回的列表中包含一个支持 model_dump() 方法的 Metrics 对象。
    """
    # 构建缓存参数
    cache_params = {'end': end_date, 'period': period}
    
    # 尝试从内存缓存获取
    cached_data = cache.get_financial_metrics(ticker)
    if cached_data:
        print(f"从内存缓存获取 {ticker} 的财务指标数据")
        return cached_data
    
    # 尝试从文件缓存获取
    file_cached_data = load_from_file_cache('financial_metrics', ticker, cache_params)
    if file_cached_data and not should_refresh_financial_data(ticker, end_date):
        # 更新内存缓存
        cache.set_financial_metrics(ticker, file_cached_data)
        print(f"从文件缓存获取 {ticker} 的财务指标数据")
        return file_cached_data
    
    # 如果缓存中没有或需要刷新，则从 API 获取
    try:
        # 检查 API 请求限制
        check_rate_limit()
        
        overview, _ = fd.get_company_overview(symbol=ticker)
        
        check_rate_limit()
        income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        
        check_rate_limit()
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        
        check_rate_limit()
        cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
        
        try:
            operating_cash = float(cash_flow["operatingCashflow"].iloc[0])
            capex = float(cash_flow["capitalExpenditures"].iloc[0])
            shares = float(overview["SharesOutstanding"].iloc[0])
            free_cash_flow = operating_cash - capex
            free_cash_flow_per_share = free_cash_flow / shares if shares != 0 else 0
        except Exception as e:
            print(f"Error calculating free cash flow per share: {str(e)}")
            free_cash_flow_per_share = 0
        
        # 获取财报日期
        report_date = None
        if "fiscalDateEnding" in income_stmt.columns:
            report_date = income_stmt["fiscalDateEnding"].iloc[0]
        
        metrics_data = {
            "return_on_equity": float(overview["ReturnOnEquityTTM"].iloc[0]) if "ReturnOnEquityTTM" in overview.columns else 0,
            "net_margin": float(overview["ProfitMargin"].iloc[0]) if "ProfitMargin" in overview.columns else 0,
            "operating_margin": float(overview["OperatingMarginTTM"].iloc[0]) if "OperatingMarginTTM" in overview.columns else 0,
            "revenue_growth": calculate_growth(income_stmt, "totalRevenue") if "totalRevenue" in income_stmt.columns else 0,
            "earnings_growth": calculate_growth(income_stmt, "netIncome") if "netIncome" in income_stmt.columns else 0,
            "book_value_growth": calculate_growth(balance_sheet, "totalStockholdersEquity") if "totalStockholdersEquity" in balance_sheet.columns else 0,
            "current_ratio": float(overview["CurrentRatio"].iloc[0]) if "CurrentRatio" in overview.columns else 0,
            "debt_to_equity": float(overview["DebtToEquityRatio"].iloc[0]) if "DebtToEquityRatio" in overview.columns else 0,
            "price_to_earnings_ratio": float(overview["PERatio"].iloc[0]) if "PERatio" in overview.columns else 0,
            "price_to_book_ratio": float(overview["PriceToBookRatio"].iloc[0]) if "PriceToBookRatio" in overview.columns else 0,
            "price_to_sales_ratio": float(overview["PriceToSalesRatioTTM"].iloc[0]) if "PriceToSalesRatioTTM" in overview.columns else 0,
            "earnings_per_share": float(overview["EPS"].iloc[0]) if "EPS" in overview.columns else 0,
            "free_cash_flow_per_share": free_cash_flow_per_share,
            "report_period": report_date or datetime.now().strftime('%Y-%m-%d')
        }
        metrics = MetricsWrapper(metrics_data)
        result = [metrics]
        
        # 保存到缓存
        cache.set_financial_metrics(ticker, result)
        save_to_file_cache('financial_metrics', ticker, result, cache_params)
        
        return result
    except Exception as e:
        print(f"Error fetching financial metrics for {ticker}: {str(e)}")
        default_data = {
            "return_on_equity": 0,
            "net_margin": 0,
            "operating_margin": 0,
            "revenue_growth": 0,
            "earnings_growth": 0,
            "book_value_growth": 0,
            "current_ratio": 0,
            "debt_to_equity": 0,
            "price_to_earnings_ratio": 0,
            "price_to_book_ratio": 0,
            "price_to_sales_ratio": 0,
            "earnings_per_share": 0,
            "free_cash_flow_per_share": 0,
            "report_period": datetime.now().strftime('%Y-%m-%d')
        }
        return [MetricsWrapper(default_data)]

def search_line_items(ticker: str, line_items: list, end_date: str = None, period: str = "ttm", limit: int = 2) -> list:
    """使用 Alpha Vantage 获取指定财报项目

    函数从年报数据中抽取所需的项目（如自由现金流、净利润、收入、经营利润率等），
    返回包含属性访问的 FinancialData 对象列表。
    """
    # 构建缓存参数
    cache_params = {'end': end_date, 'period': period, 'items': '_'.join(line_items)}
    
    # 尝试从内存缓存获取
    cached_data = cache.get_line_items(ticker)
    if cached_data:
        # 检查是否包含所有需要的项目
        all_items_present = True
        for item in cached_data:
            for line_item in line_items:
                if not hasattr(item, line_item):
                    all_items_present = False
                    break
            if not all_items_present:
                break
        
        if all_items_present:
            print(f"从内存缓存获取 {ticker} 的财报项目数据")
            return cached_data[:limit]
    
    # 尝试从文件缓存获取
    file_cached_data = load_from_file_cache('line_items', ticker, cache_params)
    if file_cached_data and not should_refresh_financial_data(ticker, end_date):
        # 更新内存缓存
        cache.set_line_items(ticker, file_cached_data)
        print(f"从文件缓存获取 {ticker} 的财报项目数据")
        return file_cached_data[:limit]
    
    # 如果缓存中没有或需要刷新，则从 API 获取
    try:
        # 检查 API 请求限制
        check_rate_limit()
        income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        
        check_rate_limit()
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        
        check_rate_limit()
        cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
        
        check_rate_limit()
        overview, _ = fd.get_company_overview(symbol=ticker)

        results = []
        for i in range(min(limit, len(income_stmt))):
            data = {}
            
            # 获取财报日期
            if "fiscalDateEnding" in income_stmt.columns:
                data["report_period"] = income_stmt["fiscalDateEnding"].iloc[i]
            else:
                data["report_period"] = datetime.now().strftime('%Y-%m-%d')
            for item in line_items:
                if item == "free_cash_flow":
                    try:
                        operating_cash = float(cash_flow["operatingCashflow"].iloc[i])
                        capex = float(cash_flow["capitalExpenditures"].iloc[i])
                        data[item] = operating_cash - capex
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "net_income":
                    try:
                        data[item] = float(income_stmt["netIncome"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "earnings_per_share":
                    try:
                        net_income = float(income_stmt["netIncome"].iloc[i])
                        shares = 0
                        if "commonStockSharesOutstanding" in balance_sheet.columns:
                            shares = float(balance_sheet["commonStockSharesOutstanding"].iloc[i])
                        else:
                            overview, _ = fd.get_company_overview(symbol=ticker)
                            if "SharesOutstanding" in overview.columns:
                                shares = float(overview["SharesOutstanding"].iloc[0])
                        data[item] = net_income / shares if shares != 0 else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "book_value_per_share":
                    try:
                        equity = float(balance_sheet["totalShareholderEquity"].iloc[i])
                        shares = 0
                        if "commonStockSharesOutstanding" in balance_sheet.columns:
                            shares = float(balance_sheet["commonStockSharesOutstanding"].iloc[i])
                        data[item] = equity / shares if shares != 0 else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "current_assets":
                    try:
                        data[item] = float(balance_sheet["totalCurrentAssets"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "current_liabilities":
                    try:
                        data[item] = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "depreciation_and_amortization":
                    try:
                        data[item] = float(cash_flow["depreciationDepletionAndAmortization"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "capital_expenditure":
                    try:
                        data[item] = float(cash_flow["capitalExpenditures"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "working_capital":
                    try:
                        current_assets = float(balance_sheet["totalCurrentAssets"].iloc[i])
                        current_liabilities = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                        data[item] = current_assets - current_liabilities
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "revenue":
                    try:
                        data[item] = float(income_stmt["totalRevenue"].iloc[i]) if "totalRevenue" in income_stmt.columns else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "operating_margin":
                    try:
                        if "operatingIncome" in income_stmt.columns and "totalRevenue" in income_stmt.columns:
                            total_revenue = float(income_stmt["totalRevenue"].iloc[i])
                            operating_income = float(income_stmt["operatingIncome"].iloc[i])
                            data[item] = operating_income / total_revenue if total_revenue != 0 else 0
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "debt_to_equity":
                    try:
                        if "totalLiabilities" in balance_sheet.columns and "totalShareholderEquity" in balance_sheet.columns:
                            total_liabilities = float(balance_sheet["totalLiabilities"].iloc[i])
                            stockholders_equity = float(balance_sheet["totalShareholderEquity"].iloc[i])
                            data[item] = total_liabilities / stockholders_equity if stockholders_equity != 0 else 0
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "total_assets":
                    try:
                        data[item] = float(balance_sheet["totalAssets"].iloc[i]) if "totalAssets" in balance_sheet.columns else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "total_liabilities":
                    try:
                        data[item] = float(balance_sheet["totalLiabilities"].iloc[i]) if "totalLiabilities" in balance_sheet.columns else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "dividends_and_other_cash_distributions":
                    data[item] = 0  # ALPHA VANTAGE 暂未提供此项数据
                # 添加缺失的项目处理
                elif item == "outstanding_shares":
                    try:
                        if "commonStockSharesOutstanding" in balance_sheet.columns:
                            data[item] = float(balance_sheet["commonStockSharesOutstanding"].iloc[i])
                        else:
                            if "SharesOutstanding" in overview.columns:
                                data[item] = float(overview["SharesOutstanding"].iloc[0])
                            else:
                                data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "operating_income":
                    try:
                        if "operatingIncome" in income_stmt.columns:
                            data[item] = float(income_stmt["operatingIncome"].iloc[i])
                        elif "totalRevenue" in income_stmt.columns and "operatingExpenses" in income_stmt.columns:
                            revenue = float(income_stmt["totalRevenue"].iloc[i])
                            operating_expenses = float(income_stmt["operatingExpenses"].iloc[i])
                            data[item] = revenue - operating_expenses
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "return_on_invested_capital":
                    try:
                        if "netIncome" in income_stmt.columns and "totalAssets" in balance_sheet.columns and "totalCurrentLiabilities" in balance_sheet.columns:
                            net_income = float(income_stmt["netIncome"].iloc[i])
                            total_assets = float(balance_sheet["totalAssets"].iloc[i])
                            current_liabilities = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                            invested_capital = total_assets - current_liabilities
                            data[item] = net_income / invested_capital if invested_capital != 0 else 0
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "cash_and_equivalents":
                    try:
                        if "cashAndCashEquivalentsAtCarryingValue" in balance_sheet.columns:
                            data[item] = float(balance_sheet["cashAndCashEquivalentsAtCarryingValue"].iloc[i])
                        elif "cashAndShortTermInvestments" in balance_sheet.columns:
                            data[item] = float(balance_sheet["cashAndShortTermInvestments"].iloc[i])
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "total_debt":
                    try:
                        short_term_debt = 0
                        long_term_debt = 0
                        if "shortTermDebt" in balance_sheet.columns:
                            short_term_debt = float(balance_sheet["shortTermDebt"].iloc[i])
                        if "longTermDebt" in balance_sheet.columns:
                            long_term_debt = float(balance_sheet["longTermDebt"].iloc[i])
                        data[item] = short_term_debt + long_term_debt
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "shareholders_equity":
                    try:
                        if "totalShareholderEquity" in balance_sheet.columns:
                                                        data[item] = float(balance_sheet["totalShareholderEquity"].iloc[i])
                        elif "totalStockholdersEquity" in balance_sheet.columns:
                            data[item] = float(balance_sheet["totalStockholdersEquity"].iloc[i])
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "operating_expense":
                    try:
                        if "operatingExpenses" in income_stmt.columns:
                            data[item] = float(income_stmt["operatingExpenses"].iloc[i])
                        elif "totalOperatingExpenses" in income_stmt.columns:
                            data[item] = float(income_stmt["totalOperatingExpenses"].iloc[i])
                        elif "totalRevenue" in income_stmt.columns and "operatingIncome" in income_stmt.columns:
                            revenue = float(income_stmt["totalRevenue"].iloc[i])
                            op_income = float(income_stmt["operatingIncome"].iloc[i])
                            data[item] = revenue - op_income
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "ebit":
                    try:
                        # 使用营业利润率和总收入计算 EBIT
                        operating_margin = float(overview["OperatingMarginTTM"].iloc[0])
                        revenue = float(overview["RevenueTTM"].iloc[0])
                        data[item] = operating_margin * revenue
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "ebitda":
                    try:
                        # 直接从 overview 获取
                        if "EBITDA" in overview.columns:
                            data[item] = float(overview["EBITDA"].iloc[0])
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0

                elif item == "goodwill_and_intangible_assets":
                    try:
                        goodwill = 0
                        intangible_assets = 0
                        if "goodwill" in balance_sheet.columns:
                            goodwill_val = balance_sheet["goodwill"].iloc[i]
                            if goodwill_val and goodwill_val != 'None':
                                goodwill = float(goodwill_val)
                        if "intangibleAssets" in balance_sheet.columns:
                            intangible_val = balance_sheet["intangibleAssets"].iloc[i]
                            if intangible_val and intangible_val != 'None':
                                intangible_assets = float(intangible_val)
                        data[item] = goodwill + intangible_assets
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "gross_margin":
                    try:
                        total_revenue = float(income_stmt["totalRevenue"].iloc[i]) if "totalRevenue" in income_stmt.columns else 0
                        if total_revenue:
                            if "grossProfit" in income_stmt.columns:
                                gross_profit = float(income_stmt["grossProfit"].iloc[i])
                                data[item] = gross_profit / total_revenue
                            elif "costOfRevenue" in income_stmt.columns:
                                cost_rev = float(income_stmt["costOfRevenue"].iloc[i])
                                data[item] = (total_revenue - cost_rev) / total_revenue
                            else:
                                data[item] = 0
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "research_and_development":
                    try:
                        if "researchAndDevelopment" in income_stmt.columns:
                            data[item] = float(income_stmt["researchAndDevelopment"].iloc[i])
                        elif "researchAndDevelopmentExpense" in income_stmt.columns:
                            data[item] = float(income_stmt["researchAndDevelopmentExpense"].iloc[i])
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                else:
                    print(f"Warning: Unknown line item {item}")
            results.append(MetricsWrapper(data))
        return results
    except Exception as e:
        print(f"Error fetching line items for {ticker}: {str(e)}")
        return []
        
def get_insider_trades(ticker: str, end_date: str, start_date: str = None, limit: int = 1000) -> list:
    """使用 Alpha Vantage 获取内部交易数据，并根据交易日期过滤结果

    调用 ALPHAVANTAGE 的 INSIDER_TRANSACTIONS 接口返回所有内部交易数据，
    根据传入的 start_date 和 end_date 与每条交易中的 transaction_date 进行比较，
    过滤出在指定日期范围内的交易信息。

    参数：
      ticker      - 股票代码
      end_date    - 截止日期（格式：YYYY-MM-DD）
      start_date  - 起始日期（格式：YYYY-MM-DD），若为 None，则不过滤起始日期
      limit       - 返回的记录条数上限，默认为 1000（在满足条件的数据中截取）
    """
    # 构建缓存参数
    cache_params = {'start': start_date, 'end': end_date, 'limit': limit}
    
    # 尝试从内存缓存获取
    cached_data = cache.get_insider_trades(ticker)
    if cached_data:
        # 过滤日期范围
        filtered_data = []
        for trade in cached_data:
            trade_date = getattr(trade, 'date', '')
            if not trade_date:
                continue
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            filtered_data.append(trade)
        
        if filtered_data:
            # 根据 limit 参数截取结果列表
            if limit and len(filtered_data) > limit:
                filtered_data = filtered_data[:limit]
            print(f"从内存缓存获取 {ticker} 的内部交易数据")
            return filtered_data
    
    # 尝试从文件缓存获取
    file_cached_data = load_from_file_cache('insider_trades', ticker, cache_params)
    if file_cached_data:
        # 更新内存缓存
        cache.set_insider_trades(ticker, file_cached_data)
        print(f"从文件缓存获取 {ticker} 的内部交易数据")
        return file_cached_data
    
    # 如果缓存中没有，则从 API 获取
    try:
        # 检查 API 请求限制
        check_rate_limit()
        
        url = f'https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}'
        response = requests.get(url)
        data = response.json()
        
        if not data or 'data' not in data:
            print(f"No insider trade data available for {ticker}")
            return []
        
        trades = data['data']
        formatted_trades = []
        
        # 遍历所有返回的内部交易记录
        for trade in trades:
            transaction_date = trade.get('transaction_date', '')
            # 如果交易日期为空，则跳过该条记录
            if not transaction_date:
                continue
            # 直接利用字符串比较（由于格式均为 YYYY-MM-DD）
            if start_date and transaction_date < start_date:
                continue
            if end_date and transaction_date > end_date:
                continue
            try:
                is_sale = (trade.get('acquisition_or_disposal', '') == 'D')
                share_price_val = trade.get('share_price', '0')
                share_price = float(share_price_val) if share_price_val and share_price_val != '' else 0.0
                shares = float(trade.get('shares', 0) or 0)
                filing_date = trade.get('filing_date', transaction_date)
                
                formatted_trade = type('InsiderTrade', (), {
                    'date': transaction_date,
                    'filing_date': filing_date,  # 添加 filing_date 用于缓存键
                    'insider_name': trade.get('executive', ''),
                    'insider_title': trade.get('executive_title', ''),
                    'transaction_type': 'sell' if is_sale else 'buy',
                    'price': share_price,
                    'transaction_shares': shares,
                    'value': share_price * shares,
                    'shares_owned': 0,
                })()
                formatted_trades.append(formatted_trade)
            except Exception as e:
                print(f"Error processing trade: {str(e)}")
                continue
        
        # 根据 limit 参数截取结果列表
        if limit and len(formatted_trades) > limit:
            formatted_trades = formatted_trades[:limit]
        
        # 保存到缓存
        cache.set_insider_trades(ticker, formatted_trades)
        save_to_file_cache('insider_trades', ticker, formatted_trades, cache_params)
        
        print(f"\nDebug - Formatted {len(formatted_trades)} insider trades successfully")
        return formatted_trades
    except Exception as e:
        print(f"Error fetching insider trades for {ticker}: {str(e)}")
        return []

# 添加内存缓存支持
_market_cap_cache: dict[str, float] = {}

def get_market_cap(ticker: str, end_date: str = None) -> float:
    """使用 Alpha Vantage 获取市值数据"""
    # 构建缓存键
    cache_key = f"{ticker}_{end_date}" if end_date else ticker
    
    # 尝试从内存缓存获取
    if cache_key in _market_cap_cache:
        print(f"从内存缓存获取 {ticker} 的市值数据")
        return _market_cap_cache[cache_key]
    
    # 构建缓存参数
    cache_params = {'end': end_date}
    
    # 尝试从文件缓存获取
    file_cached_data = load_from_file_cache('market_cap', ticker, cache_params)
    if file_cached_data is not None:
        # 更新内存缓存
        _market_cap_cache[cache_key] = file_cached_data
        print(f"从文件缓存获取 {ticker} 的市值数据")
        return file_cached_data
    
    # 如果缓存中没有，则从 API 获取
    try:
        # 检查 API 请求限制
        check_rate_limit()
        
        overview, _ = fd.get_company_overview(symbol=ticker)
        market_cap = overview.get("MarketCapitalization")
        if market_cap is not None:
            result = float(market_cap.iloc[0])
            
            # 保存到缓存
            _market_cap_cache[cache_key] = result
            save_to_file_cache('market_cap', ticker, result, cache_params)
            
            return result
        return 0
    except Exception as e:
        print(f"Error fetching market cap for {ticker}: {str(e)}")
        return 0

def prices_to_df(prices: list[dict]) -> pd.DataFrame:
    """将价格数据转换为 DataFrame

    将 price 字典列表转换为 DataFrame，并根据时间字段设置索引。
    """
    df = pd.DataFrame(prices)
    if not df.empty and "time" in df.columns:
        df["Date"] = pd.to_datetime(df["time"])
        df.set_index("Date", inplace=True)
        numeric_cols = ["open", "high", "low", "close", "adjusted_close", "volume", "dividend_amount", "split_coefficient"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df.sort_index(inplace=True)
    return df

def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取价格数据并转换为 DataFrame"""
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)

class MetricsWrapper:
    """
    用于包装财务指标数据，使之支持 model_dump() 方法（类似 pydantic 对象）
    """
    def __init__(self, data: dict):
        self.__dict__.update(data)
    def model_dump(self):
        return self.__dict__

class CompanyNews:
    """
    封装 Alpha Vantage 新闻数据，使新闻项支持属性访问。
    例如，可以使用 news.sentiment 访问新闻情感数据。
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    def model_dump(self):
        return self.__dict__

class CompanyNews:
    """
    封装 Alpha Vantage 新闻数据，使新闻项支持属性访问。
    映射字段说明：
      - time_published 映射为 date（仅保留日期部分）
      - overall_sentiment_score 映射为 sentiment（转换为 float 类型）
    例如，可以使用 news.sentiment 访问新闻情感数据，使用 news.date 进行日期过滤。
    """
    def __init__(self, **kwargs):
        # 将 time_published 映射为 date（只取日期部分，“T”或空格分隔）
        tp = kwargs.get("time_published", "")
        if tp:
            if "T" in tp:
                self.date = tp.split("T")[0]
            else:
                self.date = tp.split(" ")[0]
        else:
            self.date = ""
        # 将 overall_sentiment_score 映射为 sentiment，并转换为 float 类型（若无法转换则为 None）
        s = kwargs.get("overall_sentiment_score", None)
        try:
            self.sentiment = float(s) if s is not None else None
        except Exception:
            self.sentiment = None
        # 将其他属性也添加进来，但不覆盖已有的 date 与 sentiment
        temp = {k: v for k, v in kwargs.items() if k not in ["time_published", "overall_sentiment_score"]}
        self.__dict__.update(temp)
    def model_dump(self):
        return self.__dict__

def get_company_news(ticker: str, end_date: str, start_date: str = None, limit: int = 1000) -> list:
    """
    使用 Alpha Vantage 获取公司新闻数据和情感数据，将 start_date 和 end_date 转换为 API 查询 URL 的时间参数。

    参数:
      ticker: 股票代码（例如 "AAPL"）。
      end_date: 截止日期，格式为 "YYYY-MM-DD"。
      start_date: 起始日期，格式为 "YYYY-MM-DD"。若为 None，则默认表示只查询 end_date 当天的新闻（即 start_date = end_date）。
      limit: API 返回的记录条数上限（默认 1000）。

    根据 Alpha Vantage 新闻 API 文档：
      - 可选参数 time_from 和 time_to 的格式为 YYYYMMDDTHHMM。
      - 例如，要查询 2022-04-10 当天的新闻，则 time_from=20220410T0000 和 time_to=20220410T2359。
    """
    # 如果只有一个日期传入，则补全另外一个
    if start_date is None and end_date is not None:
        start_date = end_date
    elif end_date is None and start_date is not None:
        end_date = start_date
    
    # 构建缓存参数
    cache_params = {'start': start_date, 'end': end_date, 'limit': limit}
    
    # 尝试从内存缓存获取
    cached_data = cache.get_company_news(ticker)
    if cached_data:
        # 过滤日期范围
        filtered_data = []
        for news in cached_data:
            news_date = getattr(news, 'date', '')
            if not news_date:
                continue
            if start_date and news_date < start_date:
                continue
            if end_date and news_date > end_date:
                continue
            filtered_data.append(news)
        
        if filtered_data:
            # 根据 limit 参数截取结果列表
            if limit and len(filtered_data) > limit:
                filtered_data = filtered_data[:limit]
            print(f"从内存缓存获取 {ticker} 的公司新闻数据")
            return filtered_data
    
    # 尝试从文件缓存获取
    file_cached_data = load_from_file_cache('company_news', ticker, cache_params)
    if file_cached_data:
        # 更新内存缓存
        cache.set_company_news(ticker, file_cached_data)
        print(f"从文件缓存获取 {ticker} 的公司新闻数据")
        return file_cached_data
    
    # 如果缓存中没有，则从 API 获取
    try:
        # 检查 API 请求限制
        check_rate_limit()
        
        time_from = None
        time_to = None
        if start_date and end_date:
            # 将 "YYYY-MM-DD" 格式转换为 "YYYYMMDDT0000" 和 "YYYYMMDDT2359"
            time_from = f"{start_date.replace('-', '')}T0000"
            time_to = f"{end_date.replace('-', '')}T2359"

        url = "https://www.alphavantage.co/query"
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "apikey": ALPHA_VANTAGE_API_KEY,
            "limit": limit,
            "sort": "LATEST",
        }
        if time_from:
            params["time_from"] = time_from
        if time_to:
            params["time_to"] = time_to

        response = requests.get(url, params=params)
        json_data = response.json()
        if "feed" in json_data and json_data["feed"]:
            news_items = [CompanyNews(**item) for item in json_data["feed"]]
            
            # 保存到缓存
            cache.set_company_news(ticker, news_items)
            save_to_file_cache('company_news', ticker, news_items, cache_params)
            
            return news_items
        else:
            print(f"Error fetching news for {ticker}: {json_data}")
            return []
    except Exception as e:
        print(f"Error fetching company news for {ticker}: {str(e)}")
        return []
