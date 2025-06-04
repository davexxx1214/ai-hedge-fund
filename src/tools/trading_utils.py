"""
交易相关工具函数
"""
import ntplib
from datetime import datetime, timedelta, time
from pytz import timezone
import holidays

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

def get_previous_trading_day(current_date=None):
    """获取上一个交易日"""
    if current_date is None:
        current_date = datetime.now().date()
    elif isinstance(current_date, datetime):
        current_date = current_date.date()
    
    # 获取前一天
    prev_date = current_date - timedelta(days=1)
    
    # 如果是周末，继续往前找
    while prev_date.weekday() >= 5:  # 5是周六，6是周日
        prev_date = prev_date - timedelta(days=1)
    
    # 检查是否是节假日
    try:
        us_holidays = holidays.NYSE(years=prev_date.year)
        while prev_date in us_holidays:
            prev_date = prev_date - timedelta(days=1)
            # 确保不是周末
            while prev_date.weekday() >= 5:
                prev_date = prev_date - timedelta(days=1)
    except Exception as e:
        print(f"检查节假日时出错: {e}")
    
    return prev_date

def should_use_previous_trading_day():
    """判断当前是否应该使用上一个交易日的数据"""
    eastern = timezone('US/Eastern')
    utc_time = get_network_time()
    current_datetime = utc_time.astimezone(eastern)
    current_date = current_datetime.date()
    
    print(f"DEBUG: should_use_previous_trading_day() 被调用，当前时间: {current_datetime}")
    
    # 如果是交易时间，使用当前日期的数据
    if is_market_trading_hours(current_datetime):
        print(f"DEBUG: 当前是交易时间，使用当前日期: {current_date}")
        return False, current_date
    
    # 如果是周末，使用上一个交易日的数据
    if current_date.weekday() >= 5:
        prev_day = get_previous_trading_day(current_date)
        print(f"DEBUG: 当前是周末，使用上一个交易日: {prev_day}")
        return True, prev_day
    
    # 如果是节假日，使用上一个交易日的数据
    try:
        us_holidays = holidays.NYSE(years=current_date.year)
        if current_date in us_holidays:
            prev_day = get_previous_trading_day(current_date)
            print(f"DEBUG: 当前是节假日，使用上一个交易日: {prev_day}")
            return True, prev_day
    except Exception as e:
        print(f"检查节假日时出错: {e}")
    
    # 如果是工作日但不是交易时间（盘前盘后），使用上一个交易日的数据
    prev_day = get_previous_trading_day(current_date)
    print(f"DEBUG: 当前是工作日非交易时间，使用上一个交易日: {prev_day}")
    return True, prev_day 