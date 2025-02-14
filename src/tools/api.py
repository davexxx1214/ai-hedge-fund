import os
import pandas as pd
import requests
from datetime import datetime, timedelta

from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData

# 从环境变量中获取 ALPHAVANTAGE API Key
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
# 初始化 Alpha Vantage 客户端
ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format="pandas")
fd = FundamentalData(key=ALPHA_VANTAGE_API_KEY, output_format="pandas")

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
    注意：这里将原来的“date”字段改为“time”，以便与后续转换保持一致。
    """
    try:
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
        return filtered_data.to_dict('records')
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
    try:
        overview, _ = fd.get_company_overview(symbol=ticker)
        income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
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
        }
        metrics = MetricsWrapper(metrics_data)
        return [metrics]
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
        }
        return [MetricsWrapper(default_data)]

def search_line_items(ticker: str, line_items: list, end_date: str = None, period: str = "ttm", limit: int = 2) -> list:
    """使用 Alpha Vantage 获取指定财务报表项目

    函数从年报数据中抽取所需的项目（如自由现金流、净利润、收入、经营利润率等），
    返回包含属性访问的 FinancialData 对象列表。
    """
    try:
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
                    data[item] = float(cash_flow["depreciationDepletionAndAmortization"].iloc[i])
                elif item == "capital_expenditure":
                    data[item] = float(cash_flow["capitalExpenditures"].iloc[i])
                elif item == "working_capital":
                    current_assets = float(balance_sheet["totalCurrentAssets"].iloc[i])
                    current_liabilities = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                    data[item] = current_assets - current_liabilities
                elif item == "revenue":
                    data[item] = float(income_stmt["totalRevenue"].iloc[i]) if "totalRevenue" in income_stmt.columns else 0
                elif item == "operating_margin":
                    if "operatingIncome" in income_stmt.columns and "totalRevenue" in income_stmt.columns:
                        total_revenue = float(income_stmt["totalRevenue"].iloc[i])
                        operating_income = float(income_stmt["operatingIncome"].iloc[i])
                        data[item] = operating_income / total_revenue if total_revenue != 0 else 0
                    else:
                        data[item] = 0
                elif item == "debt_to_equity":
                    if "totalLiabilities" in balance_sheet.columns and "totalStockholdersEquity" in balance_sheet.columns:
                        total_liabilities = float(balance_sheet["totalLiabilities"].iloc[i])
                        stockholders_equity = float(balance_sheet["totalStockholdersEquity"].iloc[i])
                        data[item] = total_liabilities / stockholders_equity if stockholders_equity != 0 else 0
                    else:
                        data[item] = 0
                elif item == "total_assets":
                    data[item] = float(balance_sheet["totalAssets"].iloc[i]) if "totalAssets" in balance_sheet.columns else 0
                elif item == "total_liabilities":
                    data[item] = float(balance_sheet["totalLiabilities"].iloc[i]) if "totalLiabilities" in balance_sheet.columns else 0
                elif item == "dividends_and_other_cash_distributions":
                    # ALPHA VANTAGE 暂未提供此项数据，默认返回 0
                    data[item] = 0
                elif item == "outstanding_shares":
                    # 尝试从公司概览中获取
                    overview, _ = fd.get_company_overview(symbol=ticker)
                    data[item] = float(overview["SharesOutstanding"].iloc[0]) if "SharesOutstanding" in overview.columns else 0
                else:
                    print(f"Warning: Unknown line item {item}")
                    data[item] = 0
            results.append(type('FinancialData', (), data)())
        return results
    except Exception as e:
        print(f"Error fetching line items for {ticker}: {str(e)}")
        default_data = {item: 0 for item in line_items}
        return [type('FinancialData', (), default_data)()]

def get_insider_trades(ticker: str, end_date: str = None, limit: int = 1000) -> list:
    """使用 Alpha Vantage 获取内部交易数据

    调用 ALPHAVANTAGE 的 INSIDER_TRANSACTIONS 接口，对返回的数据进行简单格式化。
    """
    try:
        url = f'https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}'
        response = requests.get(url)
        data = response.json()
        
        if not data or 'data' not in data:
            print(f"No insider trade data available for {ticker}")
            return []
        
        trades = data['data']
        formatted_trades = []
        for trade in trades:
            try:
                is_sale = trade.get('acquisition_or_disposal', '') == 'D'
                share_price = trade.get('share_price', '0')
                share_price = float(share_price) if share_price and share_price != '' else 0.0
                shares = float(trade.get('shares', 0) or 0)
                formatted_trade = type('InsiderTrade', (), {
                    'date': trade.get('transaction_date', ''),
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
        print(f"\nDebug - Formatted {len(formatted_trades)} insider trades successfully")
        return formatted_trades
    except Exception as e:
        print(f"Error fetching insider trades for {ticker}: {str(e)}")
        return []

def get_market_cap(ticker: str, end_date: str = None) -> float:
    """使用 Alpha Vantage 获取市值数据"""
    try:
        overview, _ = fd.get_company_overview(symbol=ticker)
        market_cap = overview.get("MarketCapitalization")
        if market_cap is not None:
            return float(market_cap.iloc[0])
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

def get_company_news(ticker: str, start_date: str = None, end_date: str = None, limit: int = 50) -> list:
    """使用 Alpha Vantage 获取公司新闻数据和情感数据

    此接口调用 Alpha Vantage 的 NEWS_SENTIMENT 接口，通过传入的参数过滤指定时间范围内的新闻。

    参数说明：
      - function（必需）：固定为 "NEWS_SENTIMENT"
      - tickers（可选）：例如 "AAPL"
      - topics（可选）：过滤特定新闻主题
      - time_from 和 time_to（可选）：要求的格式为 YYYYMMDDTHHMM。例如：time_from=20220410T0130
           如果传入的 start_date 和 end_date 为 "YYYY-MM-DD" 格式，则会自动转换：
             time_from = start_date 转换为 "YYYYMMDDT0000"
             time_to   = end_date   转换为 "YYYYMMDDT2359"
      - limit（可选）：返回结果的条数上限，默认 50
      - sort（可选）：可设为 "LATEST"、"EARLIEST" 或 "RELEVANCE"，默认 "LATEST"
      - apikey（必需）：你的 API Key
    
    示例调用（最简调用，不附加时间参数）：
       https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey=your_api_key
    """
    try:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "apikey": ALPHA_VANTAGE_API_KEY,
            "limit": limit,
            "sort": "LATEST",
        }
        if start_date:
            try:
                dt_from = datetime.strptime(start_date, "%Y-%m-%d")
                params["time_from"] = dt_from.strftime("%Y%m%dT0000")
            except Exception as exc:
                print(f"Invalid start_date format: {start_date}, expected YYYY-MM-DD")
        if end_date:
            try:
                dt_end = datetime.strptime(end_date, "%Y-%m-%d")
                params["time_to"] = dt_end.strftime("%Y%m%dT2359")
            except Exception as exc:
                print(f"Invalid end_date format: {end_date}, expected YYYY-MM-DD")
        
        response = requests.get(url, params=params)
        json_data = response.json()
        
        if "feed" in json_data and json_data["feed"]:
            # 使用 CompanyNews 包装返回的数据，确保每个新闻对象都有属性访问
            return [CompanyNews(**item) for item in json_data["feed"]]
        else:
            print(f"Error fetching news for {ticker}: {json_data}")
            return []
    except Exception as e:
        print(f"Error fetching company news for {ticker}: {str(e)}")
        return []
