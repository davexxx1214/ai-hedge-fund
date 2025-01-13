import os
from typing import Dict, Any, List
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
import requests

def get_prices(
    ticker: str,
    start_date: str,
    end_date: str
) -> List[Dict[str, Any]]:
    """从Alpha Vantage获取价格数据"""
    ts = TimeSeries(key=os.environ.get("ALPHAVANTAGE_API_KEY"), output_format='pandas')
    data, meta_data = ts.get_daily(symbol=ticker, outputsize='full')
    
    # 转换数据格式以匹配原API的结构
    df = data.copy()
    df.index.name = 'time'
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    
    # 按日期过滤
    mask = (df.index >= start_date) & (df.index <= end_date)
    df = df.loc[mask]
    
    # 转换为字典列表格式
    prices = df.reset_index().to_dict('records')
    if not prices:
        raise ValueError("未返回价格数据")
    return prices

def prices_to_df(prices: List[Dict[str, Any]]) -> pd.DataFrame:
    """将价格数据转换为DataFrame"""
    df = pd.DataFrame(prices)
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df

def get_price_data(
    ticker: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """获取股票价格数据并返回DataFrame"""
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)

def get_financial_metrics(ticker: str, period: str = 'quarterly', end_date: str = None) -> Dict[str, Any]:
    """从Alpha Vantage获取公司财务指标"""
    fd = FundamentalData(key=os.environ.get("ALPHAVANTAGE_API_KEY"))
    
    # 获取公司概览数据
    overview, _ = fd.get_company_overview(ticker)
    
    # 根据报告周期选择适当的方法
    if period == 'annual':
        income_stmt, _ = fd.get_income_statement_annual(ticker)
        balance_sheet, _ = fd.get_balance_sheet_annual(ticker)
    else:  # quarterly
        income_stmt, _ = fd.get_income_statement_quarterly(ticker)
        balance_sheet, _ = fd.get_balance_sheet_quarterly(ticker)
    
    # 如果提供了 end_date，过滤历史数据
    if end_date:
        if not income_stmt.empty:
            income_stmt = income_stmt[income_stmt['fiscalDateEnding'] <= end_date]
        if not balance_sheet.empty:
            balance_sheet = balance_sheet[balance_sheet['fiscalDateEnding'] <= end_date]
    
    # 提取最新的财务数据（第一行）
    latest_income = income_stmt.iloc[0].to_dict() if not income_stmt.empty else {}
    latest_balance = balance_sheet.iloc[0].to_dict() if not balance_sheet.empty else {}
    
    metrics = {
        # 市场相关指标
        'MarketCap': float(overview.get('MarketCapitalization', 0)),
        'PE': float(overview.get('PERatio', 0)),
        'PB': float(overview.get('PriceToBookRatio', 0)),
        'DividendYield': float(overview.get('DividendYield', 0)),
        
        # 财务相关指标
        'Revenue': float(latest_income.get('totalRevenue', 0)),
        'NetIncome': float(latest_income.get('netIncome', 0)),
        'TotalAssets': float(latest_balance.get('totalAssets', 0)),
        'TotalLiabilities': float(latest_balance.get('totalLiabilities', 0)),
        
        # 其他关键指标
        'ROE': float(overview.get('ReturnOnEquityTTM', 0)),
        'ProfitMargin': float(overview.get('ProfitMargin', 0)),
        'OperatingMarginTTM': float(overview.get('OperatingMarginTTM', 0)),
        'Beta': float(overview.get('Beta', 0))
    }
    
    # 清理数据：将'None'或无效值转换为0
    for key in metrics:
        if not metrics[key] or pd.isna(metrics[key]):
            metrics[key] = 0.0
    
    return metrics

def get_insider_trades(ticker: str, limit: int = 10, end_date: str = None) -> List[Dict[str, Any]]:
    """从Alpha Vantage获取公司内部交易数据
    
    Args:
        ticker: 股票代码
        limit: 返回的最大交易记录数量
        end_date: 截止日期，用于过滤历史交易数据
    """
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    url = f'https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol={ticker}&apikey={api_key}'
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if "Message" in data:
            raise ValueError(f"API错误: {data['Message']}")
            
        transactions = data.get("transactions", [])
        
        # 如果提供了end_date，过滤历史数据
        if end_date:
            transactions = [trade for trade in transactions 
                          if trade.get('transactionDate', '') <= end_date]
        
        # 转换数据格式为标准格式
        formatted_trades = []
        for trade in transactions[:limit]:
            formatted_trade = {
                'date': trade.get('transactionDate', ''),
                'insider_name': trade.get('insiderName', ''),
                'title': trade.get('insiderTitle', ''),
                'transaction_type': trade.get('transactionType', ''),
                'shares': float(trade.get('transactionShares', 0) or 0),
                'price': float(trade.get('transactionPrice', 0) or 0),
                'value': float(trade.get('transactionValue', 0) or 0)
            }
            formatted_trades.append(formatted_trade)
            
        return formatted_trades
        
    except requests.exceptions.RequestException as e:
        raise ValueError(f"获取内部交易数据失败: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"处理内部交易数据失败: {str(e)}")
    
def get_market_cap(ticker: str) -> float:
    """获取公司市值"""
    try:
        metrics = get_financial_metrics(ticker)
        return metrics['MarketCap']
    except Exception as e:
        raise ValueError(f"获取市值失败: {str(e)}")

def search_line_items(ticker: str) -> Dict[str, Any]:
    """获取公司财务报表行项目"""
    try:
        fd = FundamentalData(key=os.environ.get("ALPHAVANTAGE_API_KEY"))
        
        # 获取最新的收益报表和资产负债表
        income_stmt, _ = fd.get_income_statement_quarterly(ticker)
        balance_sheet, _ = fd.get_balance_sheet_quarterly(ticker)
        
        # 获取最新数据
        latest_income = income_stmt[0] if income_stmt else {}
        latest_balance = balance_sheet[0] if balance_sheet else {}
        
        # 合并数据
        line_items = {
            **latest_income,
            **latest_balance
        }
        
        return line_items
    except Exception as e:
        raise ValueError(f"获取财务报表行项目失败: {str(e)}")