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
    if params and 'query_date' in params:
        date_str = datetime.strptime(params['query_date'], '%Y-%m-%d').strftime('%Y%m%d')
    else:
        date_str = datetime.now().strftime('%Y%m%d')
    
    if cache_type == 'earnings':
        filename = f"{ticker}_EARNINGS_{date_str}.json"
    else:
        filename = f"{ticker}_{date_str}.json"
    
    return CACHE_DIR / cache_type / filename

def check_current_date_cache_exists(cache_type, ticker, use_trading_day_logic=False):
    """检查当前日期的缓存文件是否存在
    
    Args:
        cache_type: 缓存类型
        ticker: 股票代码
        use_trading_day_logic: 是否使用交易日逻辑，如果为True且当前不是交易时间，则检查上一个交易日的缓存
    """
    if use_trading_day_logic:
        # 使用交易日逻辑
        from src.tools.trading_utils import should_use_previous_trading_day
        use_previous, target_date = should_use_previous_trading_day()
        if use_previous:
            date_str = target_date.strftime('%Y%m%d')
            print(f"DEBUG: 检查缓存 - 使用上一个交易日 {target_date} 的缓存文件")
        else:
            date_str = datetime.now().strftime('%Y%m%d')
            print(f"DEBUG: 检查缓存 - 使用当前日期 {datetime.now().date()} 的缓存文件")
    else:
        # 使用当前日期
        date_str = datetime.now().strftime('%Y%m%d')
        print(f"DEBUG: 检查缓存 - 未使用交易日逻辑，使用当前日期 {datetime.now().date()}")
    
    cache_dir = CACHE_DIR / cache_type
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    if cache_type == 'earnings':
        cache_file = cache_dir / f"{ticker}_EARNINGS_{date_str}.json"
    else:
        cache_file = cache_dir / f"{ticker}_{date_str}.json"
    
    exists = cache_file.exists()
    print(f"DEBUG: 缓存文件 {cache_file} 存在: {exists}")
    return exists

def check_financial_cache_exists(ticker):
    """检查财务相关的所有缓存是否都存在，包括公司概览数据
    
    使用交易日逻辑：如果当前不是交易时间，检查上一个交易日的缓存
    """
    financial_cache_types = [
        'income_statement_annual',
        'income_statement_quarterly', 
        'balance_sheet_annual',
        'balance_sheet_quarterly',
        'cash_flow_annual',
        'cash_flow_quarterly',
        'company_overview'  # 添加公司概览缓存检查
    ]
    
    for cache_type in financial_cache_types:
        if not check_current_date_cache_exists(cache_type, ticker, use_trading_day_logic=True):
            return False
    
    return True

def get_target_date_for_financial_data():
    """获取财务数据的目标日期（当前日期或上一个交易日）"""
    from src.tools.trading_utils import should_use_previous_trading_day
    
    use_previous, target_date = should_use_previous_trading_day()
    if use_previous:
        print(f"当前处于交易时间或周末/节假日，将使用上一个交易日数据: {target_date}")
        return target_date.strftime('%Y-%m-%d')
    else:
        print(f"使用当前日期数据: {target_date}")
        return target_date.strftime('%Y-%m-%d')

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
    """
    仅根据EARNINGS接口缓存判断是否需要刷新财务数据
    """
    import json
    from pathlib import Path
    from datetime import datetime
    from src.data.database_core import get_db

    today_str = datetime.now().strftime('%Y%m%d')
    earnings_cache_dir = Path("src/data/cache_files/earnings")
    earnings_cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = earnings_cache_dir / f"{ticker}_EARNINGS_{today_str}.json"

    # 如果没有今日缓存，调用API获取并缓存
    if not cache_file.exists():
        try:
            from src.tools.api_base import fd
            earnings_data, _ = fd.get_earnings(symbol=ticker)
            # 保存缓存
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(earnings_data, f, ensure_ascii=False, default=str)
            print(f"已缓存 {ticker} 的EARNINGS数据到 {cache_file}")
        except Exception as e:
            print(f"获取 {ticker} 的EARNINGS数据失败: {e}")
            # 获取失败，保守起见不更新
            return False

    # 读取缓存
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            earnings_data = json.load(f)
    except Exception as e:
        print(f"读取EARNINGS缓存失败: {e}")
        return False

    # 兼容不同格式
    if isinstance(earnings_data, dict):
        earnings_data = [earnings_data]
    elif isinstance(earnings_data, list):
        pass
    else:
        # 字符串或其他类型，无法处理
        print(f"EARNINGS缓存格式异常，类型为{type(earnings_data)}, 内容为: {earnings_data}")
        return False

    # 获取数据库实例
    db = get_db()

    # 遍历EARNINGS数据，查找是否有新财报
    now = datetime.now()
    for item in earnings_data:
        if not isinstance(item, dict):
            continue
        fiscal = item.get("fiscalDateEnding")
        reported = item.get("reportedDate")
        if not fiscal or not reported:
            continue
        try:
            reported_dt = datetime.strptime(reported, "%Y-%m-%d")
            fiscal_dt = datetime.strptime(fiscal, "%Y-%m-%d")
        except:
            continue
        # 如果当前时间早于reportedDate，跳过
        if now < reported_dt:
            continue
        # 检查数据库中是否已有该财报
        existing = db.get_income_statement_quarterly(ticker)
        exists = False
        for record in existing:
            if record.get("fiscalDateEnding") == fiscal:
                exists = True
                break
        if not exists:
            # 当前时间已超过reportedDate，且数据库中没有这条财报，需更新
            return True

    # 没有发现需要更新的财报
    return False
