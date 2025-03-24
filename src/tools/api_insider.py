"""
内部交易相关API功能
"""
import requests
from datetime import datetime, timedelta, time
from pytz import timezone

from src.tools.api_base import ALPHA_VANTAGE_API_KEY, check_rate_limit
from src.tools.api_cache import save_to_file_cache
from src.data.db_cache import get_db_cache
from src.data.database import get_db
from src.data.cache import get_cache

# 内存缓存实例
cache = get_cache()

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
      
    首先尝试从SQLite数据库获取数据，如果数据库中有数据则直接返回，
    如果没有，则从API获取并更新数据库。
    """
    # 获取数据库缓存实例
    db_cache = get_db_cache()
    db = get_db()
    
    # 构建缓存参数
    cache_params = {'start': start_date, 'end': end_date, 'limit': limit}
    
    # 尝试从数据库获取
    db_data = db.get_insider_trades(ticker, start_date, end_date)
    if db_data and len(db_data) > 0:
        print(f"从数据库获取 {ticker} 的内部交易数据")
        
        # 检查数据是否完整（是否包含最新日期的数据）
        if end_date:
            latest_date = end_date
        else:
            latest_date = datetime.now().strftime('%Y-%m-%d')
        
        # 获取数据库中最新的日期
        db_latest_date = max(item.get('date', '') for item in db_data) if db_data else None
        
        # 如果数据库中的最新日期小于请求的最新日期，则需要更新数据
        need_update = True
        if db_latest_date and db_latest_date < latest_date:
            # 获取当前日期或指定的结束日期，并确保转换为美东时间
            eastern = timezone('US/Eastern')
            if end_date:
                current_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                # 使用指定的结束日期时，假设是当天结束时间
                current_datetime = datetime.combine(current_date, time(23, 59, 59)).replace(tzinfo=eastern)
            else:
                # 获取当前时间并正确转换为美东时间
                local_tz = timezone('Asia/Shanghai')  # 假设本地时区是东8区
                now = datetime.now()
                local_dt = local_tz.localize(now)
                current_datetime = local_dt.astimezone(eastern)
                current_date = current_datetime.date()
            
            print(f"当前本地时间: {now}, 转换为美东时间: {current_datetime}")
            
            # 获取数据库最新日期
            db_latest_datetime = datetime.strptime(db_latest_date, '%Y-%m-%d').date()
            
            # 判断当前是否为周末
            is_weekend = current_date.weekday() >= 5  # 5是周六，6是周日
            
            # 判断数据库最新日期是否为周五
            is_db_friday = db_latest_datetime.weekday() == 4  # 4是周五
            
            # 判断当前是否在交易时间内
            is_trading_hours = is_market_trading_hours(current_datetime)
            print(f"当前时间: {current_datetime}，是否交易时间: {is_trading_hours}")
            
            # 计算工作日差距（不考虑周末）
            business_days_diff = calculate_business_days(db_latest_datetime, current_date)
            
            # 如果当前是周末，且数据库最新日期是周五，则不需要更新
            if is_weekend and is_db_friday and (current_date - db_latest_datetime).days <= 2:
                print(f"当前是周末（{current_date}），数据库最新日期为周五（{db_latest_date}），无需更新")
                need_update = False
            # 如果当前是交易日但处于交易时间内，且数据库最新日期是最近的交易日，则不需要更新
            elif not is_weekend and is_trading_hours and business_days_diff <= 1:
                print(f"当前处于交易时间（{current_datetime}），数据库最新日期为{db_latest_date}，无需更新")
                need_update = False
            else:
                print(f"数据库中最新日期为 {db_latest_date}，需要更新到 {latest_date}")
            
            # 设置新的起始日期为数据库中最新日期的后一天
            new_start_date = (datetime.strptime(db_latest_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 从API获取增量数据
            if need_update:
                try:
                    # 检查 API 请求限制
                    check_rate_limit()
                    
                    url = f'https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}'
                    response = requests.get(url)
                    data = response.json()
                    
                    if data and 'data' in data:
                        trades = data['data']
                        new_trades = []
                        
                        # 遍历所有返回的内部交易记录
                        for trade in trades:
                            transaction_date = trade.get('transaction_date', '')
                            # 如果交易日期为空，则跳过该条记录
                            if not transaction_date:
                                continue
                            # 只获取新的数据（数据库中最新日期之后的数据）
                            if transaction_date <= db_latest_date:
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
                                    'filing_date': filing_date,
                                    'insider_name': trade.get('executive', ''),
                                    'insider_title': trade.get('executive_title', ''),
                                    'transaction_type': 'sell' if is_sale else 'buy',
                                    'price': share_price,
                                    'transaction_shares': shares,
                                    'value': share_price * shares,
                                    'shares_owned': 0,
                                })()
                                new_trades.append(formatted_trade)
                            except Exception as e:
                                print(f"Error processing trade: {str(e)}")
                                continue
                        
                        if new_trades:
                            print(f"从API获取到 {len(new_trades)} 条新的内部交易数据")
                            # 更新数据库
                            db_cache.set_insider_trades(ticker, new_trades)
                            
                            # 将新数据添加到结果中
                            for trade in new_trades:
                                trade_dict = {}
                                for attr in dir(trade):
                                    if not attr.startswith('_') and not callable(getattr(trade, attr)):
                                        trade_dict[attr] = getattr(trade, attr)
                                db_data.append(trade_dict)
                            
                            # 按日期排序
                            db_data.sort(key=lambda x: x.get('date', ''), reverse=True)
                except Exception as e:
                    print(f"更新内部交易数据时出错: {str(e)}")
        
        # 根据 limit 参数截取结果列表
        if limit and len(db_data) > limit:
            db_data = db_data[:limit]
        
        # 转换为对象
        result = []
        for item in db_data:
            if not hasattr(item, 'date'):
                # 创建具有属性访问的对象
                trade_obj = type('InsiderTrade', (), item)()
                result.append(trade_obj)
            else:
                result.append(item)
        
        return result
    
    # 如果数据库中没有数据，则从API获取
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

def is_market_trading_hours(dt: datetime) -> bool:
    """判断给定的时间是否在美股交易时间内
    
    美股交易时间为：周一至周五，东部时间9:30-16:00
    """
    # 检查是否是工作日（周一至周五）
    if dt.weekday() >= 5:  # 5是周六，6是周日
        return False
    
    # 检查时间是否在交易时间内
    market_open = time(9, 30)
    market_close = time(16, 0)
    current_time = dt.time()
    
    return market_open <= current_time <= market_close
