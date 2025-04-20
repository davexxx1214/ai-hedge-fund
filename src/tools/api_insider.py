"""
内部交易相关API功能
"""
import requests
import ntplib
from datetime import datetime, timedelta, time
from pytz import timezone
import holidays

from src.tools.api_base import ALPHA_VANTAGE_API_KEY, check_rate_limit
from src.tools.api_cache import save_to_file_cache
from src.data.db_cache import get_db_cache
from src.data.database_core import get_db
from src.data.cache import get_cache

# 内存缓存实例
cache = get_cache()

def get_insider_trades(ticker: str, query_date: str, limit: int = 1000) -> list:
    """使用 Alpha Vantage 获取内部交易数据，一次性获取全量数据（过去三十年）

    调用 ALPHAVANTAGE 的 INSIDER_TRANSACTIONS 接口返回所有内部交易数据，
    根据查询日期缓存JSON文件，并在查询时检查是否存在对应日期的缓存文件。
    如果存在缓存文件，则直接从数据库获取数据；否则从API获取全量数据并更新数据库。

    参数：
      ticker      - 股票代码
      query_date  - 查询日期（格式：YYYY-MM-DD），用于缓存文件名和判断是否需要重新获取数据
      limit       - 返回的记录条数上限，默认为 1000（在满足条件的数据中截取）
    """
    import os
    from datetime import datetime

    # 获取数据库实例
    db = get_db()
    
    # 构建缓存文件名，例如 AAPL_20250401.json
    query_date_formatted = datetime.strptime(query_date, '%Y-%m-%d').strftime('%Y%m%d')
    cache_filename = f"{ticker}_{query_date_formatted}.json"
    cache_path = os.path.join('src', 'data', 'cache_files', 'insider_trades', cache_filename)
    
    # 检查是否存在对应日期的缓存文件
    if os.path.exists(cache_path):
        print(f"发现缓存文件 {cache_filename}，直接从数据库获取 {ticker} 的内部交易数据")
        db_data = db.get_insider_trades(ticker)
        if db_data and len(db_data) > 0:
            # 根据 limit 参数截取结果列表
            if limit and len(db_data) > limit:
                db_data = db_data[:limit]
            # 转换为对象
            result = []
            for item in db_data:
                if not hasattr(item, 'date'):
                    trade_obj = type('InsiderTrade', (), item)()
                    result.append(trade_obj)
                else:
                    result.append(item)
            return result
    
    # 如果没有缓存文件，则从API获取全量数据
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
        
        # 遍历所有返回的内部交易记录，获取全量数据
        for trade in trades:
            transaction_date = trade.get('transaction_date', '')
            # 如果交易日期为空，则跳过该条记录
            if not transaction_date:
                continue
            try:
                is_sale = (trade.get('acquisition_or_disposal', '') == 'D')
                share_price_val = trade.get('share_price', '0')
                share_price = float(share_price_val) if share_price_val and share_price_val != '' else 0.0
                shares = float(trade.get('shares', 0) or 0)
                filing_date = trade.get('filing_date', transaction_date)
                insider_name = trade.get('executive', '')
                
                # 只有在有有效交易数据时才保存（例如，存在insider_name和shares）
                if insider_name and shares > 0:
                    # 改为创建字典而不是动态对象
                    formatted_trade = {
                        'date': transaction_date,
                        'filing_date': filing_date,
                        'insider_name': insider_name,
                        'insider_title': trade.get('executive_title', ''),
                        'transaction_type': 'sell' if is_sale else 'buy',
                        'price': share_price,
                        'transaction_shares': shares,
                        'value': share_price * shares,
                        'shares_owned': 0, # 注意：API可能不直接提供此信息，这里设为0
                    }
                    formatted_trades.append(formatted_trade)
            except Exception as e:
                print(f"Error processing trade: {str(e)}")
                continue
        
        # 根据 limit 参数截取结果列表
        if limit and len(formatted_trades) > limit:
            formatted_trades = formatted_trades[:limit]
        
        # 保存到文件缓存
        cache_params = {'query_date': query_date}
        save_to_file_cache('insider_trades', ticker, formatted_trades, cache_params)
        
        # 保存到数据库，先清除旧数据再全量插入
        try:
            cursor = db.conn.cursor()
            cursor.execute("DELETE FROM insider_trades WHERE ticker = ?", (ticker,))
            db.conn.commit()
            print(f"已清除 {ticker} 的旧内部交易数据")
            db.set_insider_trades(ticker, formatted_trades)
            print(f"成功将 {len(formatted_trades)} 条全量获取的 {ticker} 内部交易数据存入数据库。")
        except Exception as db_err:
            print(f"Error saving insider trades for {ticker} to database: {db_err}")
            # 不中断流程
        
        print(f"\nDebug - Formatted {len(formatted_trades)} insider trades successfully")
        return formatted_trades
    except Exception as e:
        print(f"Error fetching insider trades for {ticker}: {str(e)}")
        return []
    

def calculate_business_days(start_date, end_date):
    """计算两个日期之间的工作日数量（不包括周末）"""
    if start_date > end_date:
        return calculate_business_days(end_date, start_date)
    
    # 计算总天数
    days = (end_date - start_date).days + 1
    
    # 计算整周数量
    weeks = days // 7
    
    # 计算剩余天数
    remaining_days = days % 7
    
    # 计算起始日期的星期几（0是周一，6是周日）
    start_weekday = start_date.weekday()
    
    # 计算剩余天数中的周末天数
    weekend_days = 0
    for i in range(remaining_days):
        if (start_weekday + i) % 7 >= 5:  # 5是周六，6是周日
            weekend_days += 1
    
    # 总工作日 = 总天数 - 周末天数
    business_days = days - (weeks * 2) - weekend_days
    
    return business_days

def get_network_time():
    """从NTP服务器获取准确的网络时间"""
    try:
        client = ntplib.NTPClient()
        # 使用公共NTP服务器池
        response = client.request('pool.ntp.org', timeout=5)
        # 将NTP时间戳转换为datetime对象（UTC时间）
        # 注意：使用utcfromtimestamp而不是fromtimestamp，避免本地时区影响
        utc_time = datetime.utcfromtimestamp(response.tx_time).replace(tzinfo=timezone('UTC'))
        return utc_time
    except Exception as e:
        print(f"获取网络时间失败: {e}")
        # 如果获取网络时间失败，则使用本地时间作为备选
        print("使用本地时间作为备选")
        return datetime.now(timezone('UTC'))

def is_market_trading_hours(dt: datetime = None) -> bool:
    """判断给定的时间是否在美股交易时间内
    
    美股交易时间为：周一至周五，东部时间9:30-16:00，排除节假日
    
    参数:
        dt: 要检查的时间，如果为None则使用网络时间
    """
    # 如果没有提供时间，则获取当前网络时间
    if dt is None:
        utc_time = get_network_time()
        # 转换为美国东部时间
        eastern = timezone('US/Eastern')
        dt = utc_time.astimezone(eastern)
        print(f"当前网络时间(美东): {dt}")
    
    # 检查是否是工作日（周一至周五）
    if dt.weekday() >= 5:  # 5是周六，6是周日
        return False
    
    # 检查是否是节假日
    try:
        us_holidays = holidays.NYSE(years=dt.year)
        current_date = dt.date()
        
        if current_date in us_holidays:
            return False
    except Exception as e:
        print(f"检查节假日时出错: {e}")
        # 节假日检查失败时，继续按时间判断
    
    # 检查时间是否在交易时间内
    market_open = time(9, 30)
    market_close = time(16, 0)
    current_time = dt.time()
    
    return market_open <= current_time < market_close  # 注意：收盘时间用 <，因为 16:00:00 已经收盘
