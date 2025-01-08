import yfinance as yf
import pandas as pd
from typing import Dict, Any, List

def get_financial_metrics(
    ticker: str,
    report_period: str,
    period: str = 'ttm',
    limit: int = 1
) -> Dict[str, Any]:
    """获取财务指标"""
    yf_ticker = yf.Ticker(ticker)
    # 获取财务报表数据
    if period == 'ttm':
        financials = yf_ticker.financials
    else:
        financials = yf_ticker.quarterly_financials
    
    return financials.to_dict()

def get_prices(
    ticker: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """获取股票价格数据"""
    return yf.download(ticker, start=start_date, end=end_date)

def get_market_cap(
    ticker: str,
) -> float:
    """获取市值"""
    yf_ticker = yf.Ticker(ticker)
    info = yf_ticker.info
    return info.get('marketCap')

def get_insider_trades(
    ticker: str,
    end_date: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """获取内部交易数据"""
    yf_ticker = yf.Ticker(ticker)
    # yfinance 目前不直接提供内部交易数据
    # 如果需要这个功能，建议使用其他数据源
    raise NotImplementedError("yfinance does not provide insider trades data")

# prices_to_df 函数可以删除，因为 yfinance 直接返回 DataFrame
def get_price_data(
    ticker: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """直接获取价格数据"""
    return get_prices(ticker, start_date, end_date)