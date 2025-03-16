"""
数据库缓存模块，将SQLite数据库与现有缓存系统集成
"""

from src.data.cache import Cache, get_cache
from src.data.database import get_db

class DBCache(Cache):
    """扩展内存缓存，添加数据库持久化支持"""
    
    def __init__(self):
        super().__init__()
        self.db = get_db()
    
    def get_prices(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_prices(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_prices(ticker)
        if db_data:
            # 更新内存缓存
            super().set_prices(ticker, db_data)
            return db_data
        
        return None
    
    def set_prices(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_prices(ticker, data)
        # 更新数据库
        self.db.set_prices(ticker, data)
    
    def get_financial_metrics(self, ticker: str) -> list[dict[str, any]]:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_financial_metrics(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_financial_metrics(ticker)
        if db_data:
            # 更新内存缓存
            super().set_financial_metrics(ticker, db_data)
            return db_data
        
        return None
    
    def set_financial_metrics(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_financial_metrics(ticker, data)
        # 更新数据库
        self.db.set_financial_metrics(ticker, data)
    
    def get_line_items(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_line_items(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_line_items(ticker)
        if db_data:
            # 更新内存缓存
            super().set_line_items(ticker, db_data)
            return db_data
        
        return None
    
    def set_line_items(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_line_items(ticker, data)
        # 更新数据库
        self.db.set_line_items(ticker, data)
    
    def get_insider_trades(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_insider_trades(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_insider_trades(ticker)
        if db_data:
            # 更新内存缓存
            super().set_insider_trades(ticker, db_data)
            return db_data
        
        return None
    
    def set_insider_trades(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_insider_trades(ticker, data)
        # 更新数据库
        self.db.set_insider_trades(ticker, data)
    
    def get_company_news(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_company_news(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_company_news(ticker)
        if db_data:
            # 更新内存缓存
            super().set_company_news(ticker, db_data)
            return db_data
        
        return None
    
    def set_company_news(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_company_news(ticker, data)
        # 更新数据库
        self.db.set_company_news(ticker, data)

# 全局数据库缓存实例
_db_cache = None

def get_db_cache() -> DBCache:
    """获取全局数据库缓存实例"""
    global _db_cache
    if _db_cache is None:
        _db_cache = DBCache()
    return _db_cache
