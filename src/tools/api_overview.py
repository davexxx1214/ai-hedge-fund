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
from src.data.database import get_db
from src.data.cache import get_cache

# 内存缓存实例
cache = get_cache()
# 内存缓存字典，用于存储公司概览数据
_overview_cache = {}

def get_company_overview(ticker):
    """
    获取公司概览信息，优先从缓存获取
    
    Args:
        ticker (str): 股票代码
    
    Returns:
        dict: 公司概览信息
    """
    # 获取数据库缓存实例
    db_cache = get_db_cache()
    db = get_db()
    
    # 构建缓存键
    cache_key = f"overview_{ticker}"
    
    # 1. 尝试从内存缓存获取
    if ticker in _overview_cache:
        print(f"从内存缓存获取 {ticker} 的公司概览数据")
        return _overview_cache[ticker]
    
    # 2. 尝试从文件缓存获取
    file_cached_data = load_from_file_cache('overview', ticker, {})
    if file_cached_data is not None:
        # 更新内存缓存
        _overview_cache[ticker] = file_cached_data
        print(f"从文件缓存获取 {ticker} 的公司概览数据")
        return file_cached_data
    
    # 3. 尝试从数据库获取
    db_data = db.get_company_overview(ticker)
    if db_data:
        print(f"从数据库获取 {ticker} 的公司概览数据")
        
        # 检查数据是否需要更新（例如，如果数据超过30天未更新）
        last_updated = datetime.strptime(db_data.get('last_updated', '2000-01-01'), '%Y-%m-%d %H:%M:%S')
        current_time = datetime.now()
        days_since_update = (current_time - last_updated).days
        
        if days_since_update <= 30:  # 如果数据不超过30天
            # 更新内存缓存
            _overview_cache[ticker] = db_data
            # 更新文件缓存
            save_to_file_cache('overview', ticker, db_data, {})
            return db_data
        else:
            print(f"{ticker} 的公司概览数据已过期（{days_since_update}天），尝试更新")
    
    # 4. 从API获取
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
            
            # 保存到数据库
            db_cache.set_company_overview(ticker, data)
            
            # 更新内存缓存
            _overview_cache[ticker] = data
            
            # 更新文件缓存
            save_to_file_cache('overview', ticker, data, {})
            
            return data
        else:
            print(f"API未返回 {ticker} 的公司概览数据")
            return None
    except Exception as e:
        print(f"获取公司概览信息时出错: {e}")
        
        # 如果API获取失败但数据库中有旧数据，则返回旧数据
        if db_data:
            print(f"返回数据库中的旧数据（{days_since_update}天前）")
            return db_data
        
        return None