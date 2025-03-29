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

    def set_company_overview(self, ticker, data):
        """缓存并存储公司概览数据"""
        # 存储到数据库
        self.db.set_company_overview(ticker, data)
        
        # 存储到缓存
        cache_key = f"company_overview_{ticker}"
        self.cache.set(cache_key, data)
        
        return data

    def get_company_overview(self, ticker):
        """获取公司概览数据，优先从缓存获取"""
        # 尝试从缓存获取
        cache_key = f"company_overview_{ticker}"
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data
        
        # 从数据库获取
        data = self.db.get_company_overview(ticker)
        if data:
            # 更新缓存
            self.cache.set(cache_key, data)
        
        return data       
    
    def get_prices(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_prices(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_prices(ticker)
        if db_data is not None and len(db_data) > 0:  # 修改这里的判断条件
            # 转换DataFrame为字典列表并更新缓存
            data_list = db_data.to_dict('records')
            super().set_prices(ticker, data_list)
            return data_list
        
        return None
    
    def set_prices(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_prices(ticker, data)
        # 更新数据库
        self.db.set_prices(ticker, data)
    
    def get_income_statement_annual(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_income_statement_annual(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_income_statement_annual(ticker)
        if db_data is not None and len(db_data) > 0:  # 修改这里的判断条件
            # 转换DataFrame为字典列表并更新缓存
            data_list = db_data.to_dict('records')
            super().set_income_statement_annual(ticker, data_list)
            return data_list
        
        return None
    
    def set_income_statement_annual(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_income_statement_annual(ticker, data)
        # 更新数据库
        self.db.set_income_statement_annual(ticker, data)
    
    def get_balance_sheet_annual(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_balance_sheet_annual(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_balance_sheet_annual(ticker)
        if db_data is not None and len(db_data) > 0:  # 修改这里的判断条件
            # 转换DataFrame为字典列表并更新缓存
            data_list = db_data.to_dict('records')
            super().set_balance_sheet_annual(ticker, data_list)
            return data_list
        
        return None
    
    def set_balance_sheet_annual(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_balance_sheet_annual(ticker, data)
        # 更新数据库
        self.db.set_balance_sheet_annual(ticker, data)
    
    def get_cash_flow_annual(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_cash_flow_annual(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_cash_flow_annual(ticker)
        if db_data is not None and len(db_data) > 0:  # 修改这里的判断条件
            # 转换DataFrame为字典列表并更新缓存
            data_list = db_data.to_dict('records')
            super().set_cash_flow_annual(ticker, data_list)
            return data_list
        
        return None
    
    def set_cash_flow_annual(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_cash_flow_annual(ticker, data)
        # 更新数据库
        self.db.set_cash_flow_annual(ticker, data)
    
    def get_income_statement_quarterly(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_income_statement_quarterly(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_income_statement_quarterly(ticker)
        if db_data is not None and len(db_data) > 0:
            # 更新缓存
            super().set_income_statement_quarterly(ticker, db_data)
            return db_data
        
        return None
    
    def set_income_statement_quarterly(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_income_statement_quarterly(ticker, data)
        # 更新数据库
        self.db.set_income_statement_quarterly(ticker, data)
    
    def get_balance_sheet_quarterly(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_balance_sheet_quarterly(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_balance_sheet_quarterly(ticker)
        if db_data is not None and len(db_data) > 0:
            # 更新缓存
            super().set_balance_sheet_quarterly(ticker, db_data)
            return db_data
        
        return None
    
    def set_balance_sheet_quarterly(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_balance_sheet_quarterly(ticker, data)
        # 更新数据库
        self.db.set_balance_sheet_quarterly(ticker, data)
    
    def get_cash_flow_quarterly(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_cash_flow_quarterly(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_cash_flow_quarterly(ticker)
        if db_data is not None and len(db_data) > 0:
            # 更新缓存
            super().set_cash_flow_quarterly(ticker, db_data)
            return db_data
        
        return None
    
    def set_cash_flow_quarterly(self, ticker: str, data: list[dict[str, any]]):
        """同时更新内存缓存和数据库"""
        # 更新内存缓存
        super().set_cash_flow_quarterly(ticker, data)
        # 更新数据库
        self.db.set_cash_flow_quarterly(ticker, data)
    
    def get_insider_trades(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_insider_trades(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_insider_trades(ticker)
        if db_data is not None and len(db_data) > 0:  # 修改这里的判断条件
            # 转换DataFrame为字典列表并更新缓存
            data_list = db_data.to_dict('records')
            super().set_insider_trades(ticker, data_list)
            return data_list
        
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
        if db_data is not None and len(db_data) > 0:  # 修改这里的判断条件
            # 转换DataFrame为字典列表并更新缓存
            data_list = db_data.to_dict('records')
            super().set_company_news(ticker, data_list)
            return data_list
        
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
