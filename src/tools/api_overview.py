#!/usr/bin/env python
"""
获取公司概览信息的API模块
"""

import os
from datetime import datetime
from alpha_vantage.fundamentaldata import FundamentalData

from src.tools.api_base import check_rate_limit, fd
from src.tools.api_cache import save_to_file_cache, load_from_file_cache
from src.data.db_cache import get_db_cache
from src.data.database_core import get_db
from src.data.cache import get_cache

# (Removed module-level _overview_cache dictionary)

def get_company_overview(ticker):
    """
    获取公司概览信息，优先从缓存获取 (使用 DBCache)
    
    Args:
        ticker (str): 股票代码
    
    Returns:
        dict: 公司概览信息, or None if not found/error.
    """
    # 获取数据库缓存实例 (handles memory cache + DB)
    db_cache = get_db_cache()
    # db = get_db() # db instance is already in db_cache

    # 1. 尝试从 DBCache 获取 (它会先检查内存，再查数据库)
    cached_data = db_cache.get_company_overview(ticker)
    if cached_data:
        # 检查数据是否需要更新（例如，如果数据超过30天未更新）
        # Note: This check should ideally live within DBCache or be triggered differently
        # For now, keep the check here based on the retrieved data.
        last_updated_str = cached_data.get('last_updated', '2000-01-01 00:00:00')
        try:
            # Ensure the format includes time if present, otherwise add default time
            if len(last_updated_str.split()) == 1:
                 last_updated_str += " 00:00:00"
            last_updated = datetime.strptime(last_updated_str, '%Y-%m-%d %H:%M:%S')
            current_time = datetime.now()
            days_since_update = (current_time - last_updated).days

            if days_since_update <= 30: # 如果数据不超过30天
                print(f"从缓存/数据库获取 {ticker} 的公司概览数据 (未过期)")
                return cached_data
            else:
                print(f"{ticker} 的公司概览数据已过期（{days_since_update}天），尝试从API更新")
        except ValueError:
             print(f"警告：无法解析 last_updated 日期 '{last_updated_str}'，将尝试从API更新")
             pass # Proceed to API call if date parsing fails

    # 2. 如果缓存没有或已过期，从API获取
    print(f"尝试从API获取 {ticker} 的公司概览数据...")
    try:
        # 检查 API 请求限制
        check_rate_limit()
        
        # 使用从 api_base 导入的 fd 实例，而不是创建新实例
        data, _ = fd.get_company_overview(symbol=ticker)
        
        if data is not None:
            print(f"从API获取 {ticker} 的公司概览数据")
            
            # 将 pandas DataFrame 转换为字典
            if hasattr(data, 'to_dict'):
                data = data.to_dict()
            
            # 保存到数据库和内存缓存 (通过 DBCache)
            db_cache.set_company_overview(ticker, data)
            
            # 更新文件缓存 (Keep this for now, although ideally managed elsewhere)
            save_to_file_cache('overview', ticker, data, {})
            
            return data
        else:
            print(f"API未返回 {ticker} 的公司概览数据")
            return None
    except Exception as e:
        print(f"获取公司概览信息时出错: {e}")
        
        # 如果API获取失败但缓存中有旧数据，则返回旧数据
        if cached_data:
            print(f"API获取失败，返回缓存中的旧数据 ({days_since_update}天前)")
            return cached_data
        
        return None # Return None if API fails and no cached data exists
