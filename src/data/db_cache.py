"""
数据库缓存模块，将SQLite数据库与现有缓存系统集成
"""
import pandas as pd # <-- Revert to standard import

from src.data.cache import Cache, get_cache
from src.data.database_core import get_db

class DBCache(Cache):
    """扩展内存缓存，添加数据库持久化支持"""
    
    def __init__(self):
        super().__init__()
        # Explicitly ensure the attribute exists on the instance after super init
        if not hasattr(self, '_company_overview_cache'):
             self._company_overview_cache: dict[str, dict[str, any]] = {}
        self.db = get_db()

    def set_company_overview(self, ticker, data):
        """缓存并存储公司概览数据"""
        # 存储到数据库
        self.db.set_company_overview(ticker, data)
        
        # 存储到缓存 (使用父类的 _company_overview_cache 属性)
        cache_key = f"company_overview_{ticker}"
        self._company_overview_cache[cache_key] = data # Use self._company_overview_cache
        
        return data

    def get_company_overview(self, ticker):
        """获取公司概览数据，优先从缓存获取"""
        # 尝试从缓存获取 (使用父类的 _company_overview_cache 属性)
        cache_key = f"company_overview_{ticker}"
        cached_data = self._company_overview_cache.get(cache_key) # Use self._company_overview_cache
        if cached_data is not None:
            return cached_data
        
        # 从数据库获取
        data = self.db.get_company_overview(ticker)
        if data:
            # 更新缓存 (使用父类的 _company_overview_cache 属性)
            self._company_overview_cache[cache_key] = data # Use self._company_overview_cache
        
        return data       
    
    def get_prices(self, ticker: str) -> list[dict[str, any]] | None:
        """先从内存缓存获取，如果没有则从数据库获取"""
        # 先尝试从内存缓存获取
        cached_data = super().get_prices(ticker)
        if cached_data:
            return cached_data
        
        # 如果内存缓存没有，则从数据库获取
        db_data = self.db.get_prices(ticker)
        # --- Start Modification ---
        if db_data is not None:
            import pandas as pd # <-- Import directly before use for safety
            if isinstance(db_data, pd.DataFrame):
                if len(db_data) > 0:
                    data_list = db_data.to_dict('records')
                    super().set_prices(ticker, data_list) # Update cache
                    return data_list
                else:
                    return [] # Return empty list for empty DataFrame
            elif isinstance(db_data, list):
                 # If db already returns a list, use it directly
                 super().set_prices(ticker, db_data) # Update cache
                 return db_data
            else:
                 print(f"Warning: Unexpected data type from db.get_prices: {type(db_data)}")
                 return None
        # --- End Modification ---
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
        # --- Start Modification ---
        if db_data is not None:
            import pandas as pd # <-- Import directly before use for safety
            if isinstance(db_data, pd.DataFrame):
                if len(db_data) > 0:
                    data_list = db_data.to_dict('records')
                    super().set_income_statement_annual(ticker, data_list) # Update cache
                    return data_list
                else:
                    return [] # Return empty list for empty DataFrame
            elif isinstance(db_data, list):
                 # If db already returns a list, use it directly
                 super().set_income_statement_annual(ticker, db_data) # Update cache
                 return db_data
            else:
                 print(f"Warning: Unexpected data type from db.get_income_statement_annual: {type(db_data)}")
                 return None
        # --- End Modification ---
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
        # --- Start Modification ---
        if db_data is not None:
            import pandas as pd # <-- Import directly before use for safety
            if isinstance(db_data, pd.DataFrame):
                if len(db_data) > 0:
                    data_list = db_data.to_dict('records')
                    super().set_balance_sheet_annual(ticker, data_list) # Update cache
                    return data_list
                else:
                    return [] # Return empty list for empty DataFrame
            elif isinstance(db_data, list):
                 # If db already returns a list, use it directly
                 super().set_balance_sheet_annual(ticker, db_data) # Update cache
                 return db_data
            else:
                 print(f"Warning: Unexpected data type from db.get_balance_sheet_annual: {type(db_data)}")
                 return None
        # --- End Modification ---
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
        # --- Start Modification ---
        if db_data is not None:
            import pandas as pd # <-- Import directly before use for safety
            if isinstance(db_data, pd.DataFrame):
                if len(db_data) > 0:
                    data_list = db_data.to_dict('records')
                    super().set_cash_flow_annual(ticker, data_list) # Update cache
                    return data_list
                else:
                    return [] # Return empty list for empty DataFrame
            elif isinstance(db_data, list):
                 # If db already returns a list, use it directly
                 super().set_cash_flow_annual(ticker, db_data) # Update cache
                 return db_data
            else:
                 print(f"Warning: Unexpected data type from db.get_cash_flow_annual: {type(db_data)}")
                 return None
        # --- End Modification ---
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
        # --- Start Modification ---
        if db_data is not None:
            import pandas as pd # <-- Import directly before use for safety
            if isinstance(db_data, pd.DataFrame):
                if len(db_data) > 0:
                    data_list = db_data.to_dict('records')
                    super().set_insider_trades(ticker, data_list) # Update cache
                    return data_list
                else:
                    return [] # Return empty list for empty DataFrame
            elif isinstance(db_data, list):
                 # If db already returns a list, use it directly
                 super().set_insider_trades(ticker, db_data) # Update cache
                 return db_data
            else:
                 print(f"Warning: Unexpected data type from db.get_insider_trades: {type(db_data)}")
                 return None
        # --- End Modification ---
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
        # --- Start Modification ---
        if db_data is not None:
            import pandas as pd # <-- Import directly before use for safety
            if isinstance(db_data, pd.DataFrame):
                if len(db_data) > 0:
                    data_list = db_data.to_dict('records')
                    super().set_company_news(ticker, data_list) # Update cache
                    return data_list
                else:
                    return [] # Return empty list for empty DataFrame
            elif isinstance(db_data, list):
                 # If db already returns a list, use it directly
                 super().set_company_news(ticker, db_data) # Update cache
                 return db_data
            else:
                 print(f"Warning: Unexpected data type from db.get_company_news: {type(db_data)}")
                 return None
        # --- End Modification ---
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
