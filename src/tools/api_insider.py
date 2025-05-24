"""
内部交易相关API功能
"""
import requests
import ntplib
import json
from datetime import datetime, timedelta, time
from pytz import timezone
import holidays

from src.tools.api_base import ALPHA_VANTAGE_API_KEY, check_rate_limit
from src.tools.api_cache import save_to_file_cache
from src.data.db_cache import get_db_cache
from src.data.database_core import get_db
from src.data.cache import get_cache

import os
from datetime import datetime
from typing import Optional

# 内存缓存实例
cache = get_cache()

def get_insider_trades(ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> list:
    """使用 Alpha Vantage 获取内部交易数据，并根据日期范围从数据库查询。

    首先检查基于 end_date 的缓存文件是否存在。如果不存在，则从 Alpha Vantage API
    获取指定 ticker 的 *全部* 历史内部交易数据，将其存入以 end_date 命名的缓存文件，
    并更新数据库（先清空该 ticker 的旧数据，再插入全部新数据）。

    无论缓存文件是否存在，最后都会查询数据库，使用 start_date 和 end_date
    过滤交易记录，并应用 limit。

    参数：
      ticker      - 股票代码
      end_date    - 查询结束日期（格式：YYYY-MM-DD），也用于缓存文件名。
      start_date  - 查询开始日期（格式：YYYY-MM-DD），可选。如果提供，则用于数据库查询过滤。
      limit       - 返回的记录条数上限，默认为 1000（在满足条件的数据库数据中截取）。
    """

    # 获取数据库实例
    db = get_db()

    # --- 缓存检查与 API 数据获取 ---
    # 构建基于 end_date 的缓存文件名
    try:
        end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d')
        cache_filename = f"{ticker}_{end_date_formatted}.json"
        cache_dir = os.path.join('src', 'data', 'cache_files', 'insider_trades')
        os.makedirs(cache_dir, exist_ok=True) # 确保目录存在
        cache_path = os.path.join(cache_dir, cache_filename)
    except ValueError:
        print(f"错误：无效的日期格式 '{end_date}'。请使用 YYYY-MM-DD。")
        return []

    # 检查缓存文件是否存在，如果不存在，则从 API 获取全量数据并更新 DB 和缓存
    if not os.path.exists(cache_path):
        print(f"未找到缓存文件 {cache_filename}。正在从 Alpha Vantage 获取 {ticker} 的全量内部交易数据...")
        try:
            # 检查 API 请求限制
            check_rate_limit()
        
            url = f'https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}'
            response = requests.get(url)
            data = response.json()

            if not data or 'data' not in data:
                print(f"Alpha Vantage 未返回 {ticker} 的内部交易数据。")
                # 即使没有数据，也创建一个空的缓存文件，避免重复查询 API
                with open(cache_path, 'w') as f:
                    json.dump([], f)
                print(f"已创建空的缓存文件 {cache_filename}。")
                # 清空数据库中可能存在的旧数据
                try:
                    cursor = db.conn.cursor()
                    cursor.execute("DELETE FROM insider_trades WHERE ticker = ?", (ticker,))
                    db.conn.commit()
                    print(f"已清除数据库中 {ticker} 的旧内部交易数据。")
                except Exception as db_err:
                    print(f"清除 {ticker} 数据库数据时出错: {db_err}")
                # 返回空列表，因为没有数据
                # return [] # 不要在这里返回，需要继续执行下面的数据库查询

            else:
                trades = data['data']
                all_formatted_trades = [] # 存储从 API 获取的所有数据

                # 遍历所有返回的内部交易记录，获取全量数据
                for trade in trades:
                    transaction_date = trade.get('transaction_date', '')
                    if not transaction_date: continue # 跳过无日期记录

                    try:
                        is_sale = (trade.get('acquisition_or_disposal', '') == 'D')
                        share_price_val = trade.get('share_price', '0')
                        share_price = float(share_price_val) if share_price_val and share_price_val != '' else 0.0
                        shares = float(trade.get('shares', 0) or 0)
                        filing_date = trade.get('filing_date', transaction_date)
                        insider_name = trade.get('executive', '')

                        if insider_name and shares != 0: # 允许 shares 为负数（卖出）
                            formatted_trade = {
                                'date': transaction_date,
                                'filing_date': filing_date,
                                'insider_name': insider_name,
                                'insider_title': trade.get('executive_title', ''),
                                'transaction_type': 'sell' if is_sale else 'buy',
                                'price': share_price,
                                'transaction_shares': shares, # 保留原始正负值
                                'value': share_price * abs(shares), # value 通常是正数
                                'shares_owned': 0, # API 可能不提供
                            }
                            all_formatted_trades.append(formatted_trade)
                    except Exception as e:
                        print(f"处理交易记录时出错: {trade} - {str(e)}")
                        continue

                # 保存全量数据到文件缓存
                try:
                    with open(cache_path, 'w') as f:
                        json.dump(all_formatted_trades, f, indent=2)
                    print(f"已将 {len(all_formatted_trades)} 条 {ticker} 的全量内部交易数据保存到缓存文件 {cache_filename}。")
                except Exception as cache_err:
                    print(f"保存缓存文件 {cache_filename} 时出错: {cache_err}")

                # 保存全量数据到数据库（先清除旧数据）
                try:
                    cursor = db.conn.cursor()
                    cursor.execute("DELETE FROM insider_trades WHERE ticker = ?", (ticker,))
                    db.conn.commit()
                    print(f"已清除数据库中 {ticker} 的旧内部交易数据。")
                    if all_formatted_trades: # 只有在有数据时才插入
                        db.set_insider_trades(ticker, all_formatted_trades) # 假设此方法处理批量插入
                        print(f"成功将 {len(all_formatted_trades)} 条 {ticker} 的全量内部交易数据存入数据库。")
                except Exception as db_err:
                    print(f"保存 {ticker} 内部交易数据到数据库时出错: {db_err}")
                    # 即使数据库保存失败，也继续尝试从可能已有的旧数据中查询

        except requests.exceptions.RequestException as req_err:
            print(f"请求 Alpha Vantage API 时出错 ({ticker}): {req_err}")
            # 无法获取新数据，将尝试从数据库查询旧数据（如果存在）
        except Exception as e:
            print(f"获取或处理 {ticker} 内部交易数据时发生未知错误: {str(e)}")
            # 同样尝试从数据库查询旧数据

    # --- 数据库查询与过滤 ---
    print(f"正在从数据库查询 {ticker} 的内部交易数据 (开始: {start_date or '不限'}, 结束: {end_date})...")
    try:
        # 调用数据库方法进行过滤查询 (需要修改 db.get_insider_trades)
        db_data = db.get_insider_trades(ticker, start_date=start_date, end_date=end_date)

        if not db_data:
            print(f"数据库中未找到 {ticker} 在指定日期范围内的内部交易数据。")
            return []

        print(f"从数据库获取了 {len(db_data)} 条 {ticker} 的内部交易记录。")

        # 应用 limit
        if limit and len(db_data) > limit:
            print(f"数据超过限制 {limit}，截取前 {limit} 条。")
            db_data = db_data[:limit]

        # 转换为对象（如果需要，假设 db.get_insider_trades 返回的是字典列表或类似结构）
        # 这里假设返回的数据已经是需要的格式，或者数据库方法返回了对象
        # 如果返回的是字典，可能需要转换：
        # result = []
        # for item in db_data:
        #     # 假设 item 是字典
        #     trade_obj = type('InsiderTrade', (), item)() # 动态创建对象
        #     result.append(trade_obj)
        # return result
        
        # 将字典列表转换为可通过属性访问的对象列表
        result = []
        for item in db_data:
            # 创建一个动态对象，将字典的键值对转换为属性
            trade_obj = type('InsiderTrade', (), item)()
            result.append(trade_obj)
        
        return result

    except Exception as db_query_err:
        print(f"从数据库查询 {ticker} 内部交易数据时出错: {db_query_err}")
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
