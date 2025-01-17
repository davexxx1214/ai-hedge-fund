from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
import pandas as pd
import os
from datetime import datetime, timedelta
import time

# 获取 API key
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
# 初始化 Alpha Vantage 客户端
ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
fd = FundamentalData(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')

def get_prices(ticker: str, start_date: str, end_date: str = None):
    """获取历史价格数据"""
    try:
        # 获取日线数据
        data, meta_data = ts.get_daily_adjusted(symbol=ticker, outputsize='full')
        
        # 重置索引，将日期变成列
        data = data.reset_index()
        
        # 重命名列以匹配原有格式
        data.columns = ['date', 'open', 'high', 'low', 'close', 'adjusted_close', 
                       'volume', 'dividend_amount', 'split_coefficient']
        
        # 转换日期为字符串格式
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        
        # 过滤日期范围
        mask = (data['date'] >= start_date)
        if end_date:
            mask &= (data['date'] <= end_date)
        
        filtered_data = data[mask]
        
        # 转换为字典列表
        return filtered_data.to_dict('records')
    
    except Exception as e:
        print(f"Error fetching price data for {ticker}: {str(e)}")
        return []

def get_financial_metrics(ticker: str, end_date: str = None, period: str = "ttm", limit: int = 10):
    """获取财务指标数据"""
    try:
        # 获取公司概览数据
        overview, _ = fd.get_company_overview(symbol=ticker)
        
        # 获取基本面数据
        income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
        
        # 计算自由现金流每股
        try:
            operating_cash = float(cash_flow["operatingCashflow"].iloc[0])
            capex = float(cash_flow["capitalExpenditures"].iloc[0])
            shares = float(overview["SharesOutstanding"].iloc[0])
            free_cash_flow = operating_cash - capex
            free_cash_flow_per_share = free_cash_flow / shares if shares != 0 else 0
        except Exception as e:
            print(f"Error calculating free cash flow per share: {str(e)}")
            free_cash_flow_per_share = 0
        
        # 创建一个具有属性访问的对象
        metrics = type('Metrics', (), {
            "return_on_equity": float(overview["ReturnOnEquityTTM"].iloc[0]) if "ReturnOnEquityTTM" in overview else 0,
            "net_margin": float(overview["ProfitMargin"].iloc[0]) if "ProfitMargin" in overview else 0,
            "operating_margin": float(overview["OperatingMarginTTM"].iloc[0]) if "OperatingMarginTTM" in overview else 0,
            "revenue_growth": calculate_growth(income_stmt, "totalRevenue"),
            "earnings_growth": calculate_growth(income_stmt, "netIncome"),
            "book_value_growth": calculate_growth(balance_sheet, "totalStockholdersEquity"),
            "current_ratio": float(overview["CurrentRatio"].iloc[0]) if "CurrentRatio" in overview else 0,
            "debt_to_equity": float(overview["DebtToEquityRatio"].iloc[0]) if "DebtToEquityRatio" in overview else 0,
            "price_to_earnings_ratio": float(overview["PERatio"].iloc[0]) if "PERatio" in overview else 0,
            "price_to_book_ratio": float(overview["PriceToBookRatio"].iloc[0]) if "PriceToBookRatio" in overview else 0,
            "price_to_sales_ratio": float(overview["PriceToSalesRatioTTM"].iloc[0]) if "PriceToSalesRatioTTM" in overview else 0,
            "earnings_per_share": float(overview["EPS"].iloc[0]) if "EPS" in overview else 0,
            "free_cash_flow_per_share": free_cash_flow_per_share,  # 添加自由现金流每股
        })()
        
        return [metrics]  # 返回包含一个对象的列表
    
    except Exception as e:
        print(f"Error fetching financial metrics for {ticker}: {str(e)}")
        # 创建一个带有默认值的对象
        return [type('Metrics', (), {
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
            "free_cash_flow_per_share": 0,  # 添加默认值
        })()]
    
def get_insider_trades(ticker: str, end_date: str = None, limit: int = 1000):
    """
    注意：Alpha Vantage 目前不直接提供内部交易数据
    这里返回一个空列表，你可能需要寻找其他数据源来补充这个功能
    """
    return []

def get_market_cap(ticker: str, end_date: str = None):
    """获取市值数据"""
    try:
        overview, _ = fd.get_company_overview(symbol=ticker)
        return float(overview.get("MarketCapitalization", 0))
    except Exception as e:
        print(f"Error fetching market cap for {ticker}: {str(e)}")
        return 0

def calculate_growth(df, column_name):
    """计算增长率"""
    try:
        if column_name not in df.columns:
            return 0
        current = float(df[column_name].iloc[0])
        previous = float(df[column_name].iloc[1])
        return (current - previous) / previous if previous != 0 else 0
    except Exception as e:
        print(f"Error calculating growth for {column_name}: {str(e)}")
        return 0

def calculate_per_share(df, column_name, overview):
    """计算每股指标"""
    try:
        value = float(df[column_name].iloc[0])
        shares = float(overview.get("SharesOutstanding", 0))
        return value / shares if shares != 0 else 0
    except:
        return 0

def prices_to_df(prices_data):
    """将价格数据转换为 DataFrame"""
    df = pd.DataFrame(prices_data)
    if not df.empty:
        df.set_index(pd.to_datetime(df['date']), inplace=True)
        df = df.drop('date', axis=1)  # 删除日期列，因为已经设为索引
    return df

def search_line_items(ticker: str, line_items: list, end_date: str = None, period: str = "ttm", limit: int = 2):
    """获取指定的财务报表项目"""
    try:
        # 获取财务报表数据
        income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)

        results = []
        for i in range(min(limit, len(income_stmt))):
            data = {}
            
            for item in line_items:
                if item == "free_cash_flow":
                    operating_cash = float(cash_flow["operatingCashflow"].iloc[i])
                    capex = float(cash_flow["capitalExpenditures"].iloc[i])
                    data[item] = operating_cash - capex
                    
                elif item == "net_income":
                    data[item] = float(income_stmt["netIncome"].iloc[i])
                    
                elif item == "depreciation_and_amortization":
                    # Alpha Vantage 中使用 "depreciationDepletionAndAmortization" 作为字段名
                    data[item] = float(cash_flow["depreciationDepletionAndAmortization"].iloc[i])
                    
                elif item == "capital_expenditure":
                    data[item] = float(cash_flow["capitalExpenditures"].iloc[i])
                    
                elif item == "working_capital":
                    current_assets = float(balance_sheet["totalCurrentAssets"].iloc[i])
                    current_liabilities = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                    data[item] = current_assets - current_liabilities
                
                else:
                    print(f"Warning: Unknown line item {item}")
                    data[item] = 0
            
            # 创建一个带有属性访问的对象
            results.append(type('FinancialData', (), data)())
            
        return results
        
    except Exception as e:
        print(f"Error fetching line items for {ticker}: {str(e)}")
        return [type('FinancialData', (), {item: 0 for item in line_items})()]