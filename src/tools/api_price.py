"""
价格相关API功能
"""
import pandas as pd
from datetime import datetime, timedelta, time
from pytz import timezone
import holidays

from src.tools.api_base import ts, check_rate_limit
from src.tools.api_cache import save_to_file_cache, load_from_file_cache
from src.tools.trading_utils import get_network_time, is_market_trading_hours, calculate_business_days, should_use_previous_trading_day
from src.data.db_cache import get_db_cache
from src.data.database_core import get_db
from src.data.cache import get_cache

# 内存缓存实例
cache = get_cache()

def get_prices(ticker: str, start_date: str, end_date: str = None) -> list:
    """使用 Alpha Vantage 获取历史价格数据

    返回的数据列表中，每个记录包含字段：
    time, open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient
    注意：这里将原来的"date"字段改为"time"，以便与后续转换保持一致。
    
    首先尝试从SQLite数据库获取数据，如果数据库中有数据则直接返回，
    如果没有或数据不完整，则从API获取并更新数据库。
    
    特殊用法：当 start_date 为 "full" 时，将获取并返回所有可用的历史数据，不进行日期过滤。
    """
    # 获取数据库缓存实例
    db_cache = get_db_cache()
    db = get_db()
    
    # 构建缓存参数
    cache_params = {'start': start_date, 'end': end_date}
    
    # 尝试从数据库获取
    db_data = db.get_prices(ticker, start_date, end_date)
    if db_data and len(db_data) > 0:
        print(f"从数据库获取 {ticker} 的价格数据")
        
        # 检查数据是否完整（是否包含最新日期的数据）
        if end_date:
            # 检查是否是当前日期，如果是且非交易时间，调整期望日期
            current_date_str = datetime.now().strftime('%Y-%m-%d')
            if end_date == current_date_str:
                # 当前日期被指定为end_date，需要根据交易时间判断
                use_previous, target_date = should_use_previous_trading_day()
                if use_previous:
                    # 如果当前不是交易时间，期望的最新日期是上一个交易日
                    latest_date = target_date.strftime('%Y-%m-%d')
                    print(f"指定了当前日期但非交易时间，期望的最新日期调整为上一个交易日: {latest_date}")
                else:
                    # 如果当前是交易时间，期望的最新日期是当前日期
                    latest_date = end_date
                    print(f"指定了当前日期且是交易时间，期望的最新日期为: {latest_date}")
            else:
                # 指定了其他日期，直接使用
                latest_date = end_date
        else:
            # 如果未指定 end_date，需要根据交易时间确定期望的最新日期
            use_previous, target_date = should_use_previous_trading_day()
            if use_previous:
                # 如果当前不是交易时间，期望的最新日期是上一个交易日
                latest_date = target_date.strftime('%Y-%m-%d')
                print(f"当前非交易时间，期望的最新日期为上一个交易日: {latest_date}")
            else:
                # 如果当前是交易时间，期望的最新日期是当前日期
                latest_date = datetime.now().strftime('%Y-%m-%d')
                print(f"当前是交易时间，期望的最新日期为当前日期: {latest_date}")
        
        # 获取数据库中最新的日期
        db_latest_date = max(item['time'] for item in db_data) if db_data else None
        
        # 判断是否需要更新数据（考虑交易日）
        need_update = False
        if db_latest_date and db_latest_date < latest_date:
            # 获取美东时间
            eastern = timezone('US/Eastern')
            
            # 获取当前真实的美东时间
            utc_time = get_network_time()
            current_system_datetime = utc_time.astimezone(eastern)
            
            if end_date:
                provided_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                if provided_date == current_system_datetime.date():
                    current_date = provided_date
                    current_datetime = current_system_datetime
                else:
                    # 使用真实的当前时间，而不是23:59:59
                    current_date = provided_date
                    current_time = current_system_datetime.time()
                    current_datetime = eastern.localize(datetime.combine(provided_date, current_time))
            else:
                current_datetime = current_system_datetime
                current_date = current_system_datetime.date()
            
            print(f"当前美东时间: {current_datetime}")
            
            # 获取数据库最新日期作为日期对象
            db_latest_datetime = datetime.strptime(db_latest_date, '%Y-%m-%d').date()
            
            # 新逻辑：如果 end_date 指定为今天且当前处于交易时间，则不更新数据
            if end_date:
                provided_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                if provided_date == current_system_datetime.date() and is_market_trading_hours(current_datetime):
                    print(f"当前处于交易时间（{current_datetime}），当天交易尚未结束，故不更新数据")
                    need_update = False
                else:
                    # 传统逻辑判断
                    is_weekend = current_date.weekday() >= 5  # 5是周六，6是周日
                    is_db_friday = db_latest_datetime.weekday() == 4  # 4是周五
                    is_trading_hours = is_market_trading_hours(current_datetime)
                    print(f"当前时间: {current_datetime}，是否交易时间: {is_trading_hours}")
                    business_days_diff = calculate_business_days(db_latest_datetime, current_date)
                    if is_weekend and is_db_friday and (current_date - db_latest_datetime).days <= 2:
                        print(f"当前是周末（{current_date}），数据库最新日期为周五（{db_latest_date}），无需更新")
                        need_update = False
                    elif not is_weekend and is_trading_hours and business_days_diff <= 1:
                        print(f"当前处于交易时间（{current_datetime}），数据库最新日期为{db_latest_date}，无需更新")
                        need_update = False
                    else:
                        print(f"数据库中最新日期为 {db_latest_date}，需要更新到 {latest_date}")
                        need_update = True
            else:
                # 如果未指定 end_date，则按传统逻辑判断
                is_weekend = current_date.weekday() >= 5  # 5是周六，6是周日
                is_db_friday = db_latest_datetime.weekday() == 4  # 4是周五
                is_trading_hours = is_market_trading_hours(current_datetime)
                print(f"当前时间: {current_datetime}，是否交易时间: {is_trading_hours}")
                business_days_diff = calculate_business_days(db_latest_datetime, current_date)
                if is_weekend and is_db_friday and (current_date - db_latest_datetime).days <= 2:
                    print(f"当前是周末（{current_date}），数据库最新日期为周五（{db_latest_date}），无需更新")
                    need_update = False
                elif not is_weekend and is_trading_hours and business_days_diff <= 1:
                    print(f"当前处于交易时间（{current_datetime}），数据库最新日期为{db_latest_date}，无需更新")
                    need_update = False
                else:
                    print(f"数据库中最新日期为 {db_latest_date}，需要更新到 {latest_date}")
                    need_update = True
        
        if need_update:
            # 设置新的起始日期为数据库中最新日期的后一天
            new_start_date = (datetime.strptime(db_latest_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 从API获取增量数据
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
                
                # 只获取新的数据（数据库中最新日期之后的数据）
                mask = (data['time'] > db_latest_date)
                if end_date:
                    mask &= (data['time'] <= end_date)
                
                new_data = data.loc[mask].to_dict('records')
                
                if new_data:
                    print(f"从API获取到 {len(new_data)} 条新的价格数据")
                    # 更新数据库
                    db_cache.set_prices(ticker, new_data)
                    
                    # 合并旧数据和新数据
                    db_data.extend(new_data)
                    
                    # 按日期排序
                    db_data.sort(key=lambda x: x['time'])
            except Exception as e:
                print(f"更新价格数据时出错: {str(e)}")
        
        # 过滤日期范围
        filtered_data = [item for item in db_data if item['time'] >= start_date and (not end_date or item['time'] <= end_date)]
        return filtered_data
    
    # 如果数据库中没有数据，则从API获取
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
        if start_date == "full":
            # 如果 start_date 为 "full"，不进行日期过滤，返回所有数据
            filtered_data = data
        else:
            mask = (data['time'] >= start_date)
            if end_date:
                mask &= (data['time'] <= end_date)
            filtered_data = data.loc[mask]
        result = filtered_data.to_dict('records')
        
        if result:
            print(f"从API获取到 {len(result)} 条价格数据")
            # 保存到数据库
            db_cache.set_prices(ticker, result)
            
            # 同时保存到内存缓存和文件缓存（向后兼容）
            cache.set_prices(ticker, result)
            save_to_file_cache('prices', ticker, result, cache_params)
        
        return result
    except Exception as e:
        print(f"Error fetching price data for {ticker}: {str(e)}")
        return []
    
    
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

def get_market_cap(ticker: str, end_date: str = None) -> float:
    """使用 Alpha Vantage 获取市值数据"""
    from src.tools.api_base import fd, check_rate_limit
    from src.tools.api_cache import save_to_file_cache
    
    # 添加内存缓存支持
    _market_cap_cache = {}
    
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
