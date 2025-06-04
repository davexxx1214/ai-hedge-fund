"""
基础设置、API限制控制等
"""
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData

# 获取缓存实例和数据库实例
from src.data.cache import get_cache
from src.data.db_cache import get_db_cache
from src.data.database_core import get_db

# API 请求限制控制
REQUEST_LIMIT = 60  # 每分钟最大请求数
MIN_REQUEST_INTERVAL = 1.0  # 每次请求之间的最小间隔（秒）
request_timestamps = []  # 记录请求时间戳
last_request_time = 0  # 记录上次请求时间

# 从环境变量中获取 ALPHAVANTAGE API Key
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
# 初始化 Alpha Vantage 客户端
ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format="pandas")
fd = FundamentalData(key=ALPHA_VANTAGE_API_KEY, output_format="pandas")

# 内存缓存实例
cache = get_cache()

def check_rate_limit():
    """检查 API 请求频率限制，必要时等待"""
    global request_timestamps, last_request_time
    
    current_time = time.time()
    
    # 确保与上次请求之间有最小间隔
    time_since_last = current_time - last_request_time
    if time_since_last < MIN_REQUEST_INTERVAL:
        sleep_time = MIN_REQUEST_INTERVAL - time_since_last
        print(f"等待请求间隔: {sleep_time:.2f} 秒...")
        time.sleep(sleep_time)
        current_time = time.time()
    
    # 清理超过一分钟的时间戳
    request_timestamps = [ts for ts in request_timestamps if current_time - ts < 60]
    
    # 如果已达到限制，等待
    if len(request_timestamps) >= REQUEST_LIMIT:
        if request_timestamps:
            oldest_timestamp = min(request_timestamps)
            sleep_time = 60 - (current_time - oldest_timestamp) + 1.0  # 额外等待 1 秒以确保安全
            if sleep_time > 0:
                print(f"API 请求达到限制（{len(request_timestamps)}/{REQUEST_LIMIT}），等待 {sleep_time:.2f} 秒...")
                time.sleep(sleep_time)
                # 重新检查（递归调用）
                return check_rate_limit()
    
    # 记录新的请求时间戳和更新上次请求时间
    request_timestamps.append(current_time)
    last_request_time = current_time
    print(f"API 请求计数: {len(request_timestamps)}/{REQUEST_LIMIT}")

# 财报发布时间规律（美国公司）
# Q1（1-3月）财报：4月中旬至5月初发布
# Q2（4-6月）财报：7月中旬至8月初发布
# Q3（7-9月）财报：10月中旬至11月初发布
# Q4/年报（10-12月）财报：次年1月中旬至2月底发布

# 字段映射
FIELD_MAPPING = {
    "current_assets": "totalCurrentAssets",
    "current_liabilities": "totalCurrentLiabilities",
    "total_assets": "totalAssets",
    "total_liabilities": "totalLiabilities",
    "net_income": "netIncome",
    "operating_income": "operatingIncome",
    "revenue": "totalRevenue",
    "gross_profit": "grossProfit",
    "research_and_development": "researchAndDevelopment",
    "operating_expenses": "operatingExpenses",
    "cost_of_revenue": "costOfRevenue",
    "selling_general_administrative": "sellingGeneralAndAdministrative",
    "depreciation_and_amortization": "depreciationDepletionAndAmortization",
    "capital_expenditure": "capitalExpenditures",
    "cash_and_equivalents": "cashAndCashEquivalentsAtCarryingValue",
    "shareholders_equity": "totalShareholderEquity",
    "outstanding_shares": "commonStockSharesOutstanding"
}

def calculate_growth(df, column_name):
    import pandas as pd # <-- Add import inside function
    """计算增长率, 用于收入、净利润、股东权益等数据"""
    try:
        if column_name not in df.columns.tolist() or len(df) < 2:
            return 0
        current_series = df[column_name].iloc[0]
        previous_series = df[column_name].iloc[1]
        
        if pd.isna(current_series) or pd.isna(previous_series):
            return 0
            
        current = float(current_series)
        previous = float(previous_series)
        
        if previous == 0:
            return 0
            
        return (current - previous) / previous
    except Exception as e:
        print(f"Error calculating growth for {column_name}: {str(e)}")
        return 0
