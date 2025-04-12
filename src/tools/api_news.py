"""
公司新闻相关API功能
"""
import requests
from datetime import datetime

from src.tools.api_base import ALPHA_VANTAGE_API_KEY, check_rate_limit
from src.tools.api_cache import save_to_file_cache, load_from_file_cache
from src.tools.api_models import CompanyNews
from src.data.cache import get_cache
from src.data.database_core import get_db # <-- 添加数据库导入

# 内存缓存实例
cache = get_cache()

def get_company_news(ticker: str, end_date: str, start_date: str = None, limit: int = 1000) -> list:
    """
    使用 Alpha Vantage 获取公司新闻数据和情感数据，将 start_date 和 end_date 转换为 API 查询 URL 的时间参数。

    参数:
      ticker: 股票代码（例如 "AAPL"）。
      end_date: 截止日期，格式为 "YYYY-MM-DD"。
      start_date: 起始日期，格式为 "YYYY-MM-DD"。若为 None，则默认表示只查询 end_date 当天的新闻（即 start_date = end_date）。
      limit: API 返回的记录条数上限（默认 1000）。

    根据 Alpha Vantage 新闻 API 文档：
      - 可选参数 time_from 和 time_to 的格式为 YYYYMMDDTHHMM。
      - 例如，要查询 2022-04-10 当天的新闻，则 time_from=20220410T0000 和 time_to=20220410T2359。
    """
    # 如果只有一个日期传入，则补全另外一个
    if start_date is None and end_date is not None:
        start_date = end_date
    elif end_date is None and start_date is not None:
        end_date = start_date
    
    # 构建缓存参数
    cache_params = {'start': start_date, 'end': end_date, 'limit': limit}
    
    # 尝试从内存缓存获取
    cached_data = cache.get_company_news(ticker)
    if cached_data:
        # 过滤日期范围
        filtered_data = []
        for news in cached_data:
            news_date = getattr(news, 'date', '')
            if not news_date:
                continue
            if start_date and news_date < start_date:
                continue
            if end_date and news_date > end_date:
                continue
            filtered_data.append(news)
        
        if filtered_data:
            # 确保所有数据都是 CompanyNews 实例
            filtered_data = [CompanyNews(**item) if isinstance(item, dict) else item for item in filtered_data]
            # 根据 limit 参数截取结果列表
            if limit and len(filtered_data) > limit:
                filtered_data = filtered_data[:limit]
            print(f"从内存缓存获取 {ticker} 的公司新闻数据")
            return filtered_data
    
    # 尝试从文件缓存获取
    file_cached_data = load_from_file_cache('company_news', ticker, cache_params)
    if file_cached_data:
        # 确保所有数据都是 CompanyNews 实例
        file_cached_data = [CompanyNews(**item) if isinstance(item, dict) else item for item in file_cached_data]
        # 更新内存缓存
        cache.set_company_news(ticker, file_cached_data)
        print(f"从文件缓存获取 {ticker} 的公司新闻数据")
        return file_cached_data
    
    # 如果缓存中没有，则从 API 获取
    try:
        # 检查 API 请求限制
        check_rate_limit()
        
        time_from = None
        time_to = None
        if start_date and end_date:
            # 统一使用美东时间上午 9:30
            time_from = f"{start_date.replace('-', '')}T0930"
            time_to = f"{end_date.replace('-', '')}T0930"

        url = "https://www.alphavantage.co/query"
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "apikey": ALPHA_VANTAGE_API_KEY,
            "limit": limit,
            "sort": "LATEST",
        }
        if time_from:
            params["time_from"] = time_from
        if time_to:
            params["time_to"] = time_to
        response = requests.get(url, params=params)
        json_data = response.json()
        
        if json_data and "feed" in json_data:
            news_items = [CompanyNews(**item) for item in json_data["feed"]]
            
            # 保存到内存和文件缓存
            cache.set_company_news(ticker, news_items)
            save_to_file_cache('company_news', ticker, news_items, cache_params)
            
            # 保存到数据库
            try:
                db = get_db()
                db.set_company_news(ticker, news_items)
            except Exception as db_err:
                print(f"Error saving company news for {ticker} to database: {db_err}")
                # 不中断流程，即使数据库保存失败也继续返回数据
            
            return news_items
        else:
            print(f"Error fetching news for {ticker}: {json_data}")
            return []
    except Exception as e:
        print(f"Error fetching company news for {ticker}: {str(e)}")
        return []
