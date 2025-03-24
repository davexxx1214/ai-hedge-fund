"""
缓存相关功能
"""
import json
import time
from datetime import datetime
from pathlib import Path

# 创建本地文件缓存目录
CACHE_DIR = Path("src/data/cache_files")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def get_cache_path(cache_type, ticker, params=None):
    """获取缓存文件路径，确保文件名格式为 {股票名}_{YYYYMMDD}.json"""
    today_str = datetime.now().strftime('%Y%m%d')
    filename = f"{ticker}_{today_str}.json"
    
    return CACHE_DIR / cache_type / filename

def save_to_file_cache(cache_type, ticker, data, params=None):
    """保存数据到文件缓存"""
    cache_path = get_cache_path(cache_type, ticker, params)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # 对于不同类型的数据，可能需要不同的序列化方法
        if isinstance(data, list) and len(data) > 0:
            if hasattr(data[0], 'model_dump'):
                # 如果是对象列表，使用 model_dump 方法
                serialized_data = [item.model_dump() if hasattr(item, 'model_dump') else item for item in data]
            elif cache_type == 'insider_trades':
                # 特殊处理 insider_trades 数据
                serialized_data = []
                for item in data:
                    # 将对象的属性转换为字典
                    item_dict = {}
                    for attr in dir(item):
                        # 跳过私有属性和方法
                        if not attr.startswith('_') and not callable(getattr(item, attr)):
                            item_dict[attr] = getattr(item, attr)
                    serialized_data.append(item_dict)
            else:
                # 其他情况直接保存
                serialized_data = data
        else:
            # 其他情况直接保存
            serialized_data = data
        
        # 确保覆盖旧文件
        if cache_path.exists():
            cache_path.unlink()

        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(serialized_data, f, ensure_ascii=False, default=str)
        
        print(f"已缓存 {ticker} 的 {cache_type} 数据到 {cache_path}")
        return True
    except Exception as e:
        print(f"缓存 {ticker} 的 {cache_type} 数据失败: {str(e)}")
        return False

def load_from_file_cache(cache_type, ticker, params=None, max_age_days=30):
    """从文件缓存加载数据"""
    from src.tools.api_models import MetricsWrapper, CompanyNews
    
    cache_path = get_cache_path(cache_type, ticker, params)
    
    if not cache_path.exists():
        return None
    
    # 检查缓存文件是否为当前日期
    today_str = datetime.now().strftime('%Y%m%d')
    if today_str in cache_path.name:
        print(f"使用当前日期的缓存文件 {cache_path}")
        with open(cache_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # 根据缓存类型处理数据
            if cache_type == 'financial_metrics':
                return [MetricsWrapper(item) for item in data]
            elif cache_type == 'line_items':
                return [MetricsWrapper(item) for item in data]
            elif cache_type == 'insider_trades':
                # 创建具有属性访问的对象
                return [type('InsiderTrade', (), item if isinstance(item, dict) else {'error': 'Invalid data format'})() for item in data]
            elif cache_type == 'company_news':
                # 创建 CompanyNews 对象
                return [CompanyNews(**item) for item in data]
            else:
                return data

    # 如果缓存文件过期，则返回 None
    file_age = (datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)).days
    if file_age > max_age_days:
        print(f"缓存文件 {cache_path} 已过期 ({file_age} 天)，重新获取数据")
        return None
    
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 根据缓存类型处理数据
        if cache_type == 'financial_metrics':
            return [MetricsWrapper(item) for item in data]
        elif cache_type == 'line_items':
            return [MetricsWrapper(item) for item in data]
        elif cache_type == 'insider_trades':
            # 创建具有属性访问的对象
            return [type('InsiderTrade', (), item if isinstance(item, dict) else {'error': 'Invalid data format'})() for item in data]
        elif cache_type == 'company_news':
            # 创建 CompanyNews 对象
            return [CompanyNews(**item) for item in data]
        else:
            return data
    except Exception as e:
        print(f"加载 {ticker} 的 {cache_type} 缓存数据失败: {str(e)}")
        return None

def should_refresh_financial_data(ticker, end_date=None):
    """判断是否应该刷新财务数据（基于财报发布时间规律）"""
    if end_date:
        # 如果提供了特定日期，检查该日期是否在财报发布期间
        target_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        target_date = datetime.now()
    
    month = target_date.month
    day = target_date.day
    
    # 判断是否在财报发布期
    if (month == 4 and day >= 15) or (month == 5 and day <= 10):  # Q1财报期
        return True
    elif (month == 7 and day >= 15) or (month == 8 and day <= 10):  # Q2财报期
        return True
    elif (month == 10 and day >= 15) or (month == 11 and day <= 10):  # Q3财报期
        return True
    elif (month == 1 and day >= 15) or (month == 2):  # Q4/年报财报期
        return True
    
    # 检查缓存文件是否存在
    cache_path = get_cache_path('financial_metrics', ticker)
    if not cache_path.parent.exists() or not cache_path.exists():
        return True  # 如果缓存不存在，需要刷新
    
    return False  # 其他情况不需要刷新
