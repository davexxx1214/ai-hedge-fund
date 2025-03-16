class Cache:
    """In-memory cache for API responses.
    
    注意：此类仅提供内存缓存功能，持久化存储请使用DBCache类。
    """

    def __init__(self):
        self._prices_cache: dict[str, list[dict[str, any]]] = {}
        self._financial_metrics_cache: dict[str, list[dict[str, any]]] = {}
        self._line_items_cache: dict[str, list[dict[str, any]]] = {}
        self._insider_trades_cache: dict[str, list[dict[str, any]]] = {}
        self._company_news_cache: dict[str, list[dict[str, any]]] = {}

    def _merge_data(self, existing: list | None, new_data: list, key_field: str) -> list:
        """Merge existing and new data, avoiding duplicates based on a key field."""
        if not existing:
            return new_data
        
        # Create a set of existing keys for O(1) lookup
        existing_keys = set()
        for item in existing:
            # 处理对象和字典两种情况
            if hasattr(item, key_field):
                # 如果是对象，使用 getattr 获取属性
                existing_keys.add(getattr(item, key_field))
            elif isinstance(item, dict) and key_field in item:
                # 如果是字典，使用 [] 获取属性
                existing_keys.add(item[key_field])
        
        # Only add items that don't exist yet
        merged = existing.copy()
        for item in new_data:
            # 同样处理对象和字典两种情况
            if hasattr(item, key_field):
                key_value = getattr(item, key_field)
                if key_value not in existing_keys:
                    merged.append(item)
            elif isinstance(item, dict) and key_field in item:
                key_value = item[key_field]
                if key_value not in existing_keys:
                    merged.append(item)
            else:
                # 如果既不是对象也不是字典，或者没有指定的键，直接添加
                merged.append(item)
        
        return merged

    def get_prices(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached price data if available."""
        return self._prices_cache.get(ticker)

    def set_prices(self, ticker: str, data: list[dict[str, any]]):
        """Append new price data to cache."""
        self._prices_cache[ticker] = self._merge_data(
            self._prices_cache.get(ticker),
            data,
            key_field="time"
        )

    def get_financial_metrics(self, ticker: str) -> list[dict[str, any]]:
        """Get cached financial metrics if available."""
        return self._financial_metrics_cache.get(ticker)

    def set_financial_metrics(self, ticker: str, data: list[dict[str, any]]):
        """Append new financial metrics to cache."""
        self._financial_metrics_cache[ticker] = self._merge_data(
            self._financial_metrics_cache.get(ticker),
            data,
            key_field="report_period"
        )

    def get_line_items(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached line items if available."""
        return self._line_items_cache.get(ticker)

    def set_line_items(self, ticker: str, data: list[dict[str, any]]):
        """Append new line items to cache."""
        self._line_items_cache[ticker] = self._merge_data(
            self._line_items_cache.get(ticker),
            data,
            key_field="report_period"
        )

    def get_insider_trades(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached insider trades if available."""
        return self._insider_trades_cache.get(ticker)

    def set_insider_trades(self, ticker: str, data: list[dict[str, any]]):
        """Append new insider trades to cache."""
        self._insider_trades_cache[ticker] = self._merge_data(
            self._insider_trades_cache.get(ticker),
            data,
            key_field="filing_date"  # Could also use transaction_date if preferred
        )

    def get_company_news(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached company news if available."""
        return self._company_news_cache.get(ticker)

    def set_company_news(self, ticker: str, data: list[dict[str, any]]):
        """Append new company news to cache."""
        self._company_news_cache[ticker] = self._merge_data(
            self._company_news_cache.get(ticker),
            data,
            key_field="date"
        )


# Global cache instance
_cache = Cache()


def get_cache() -> Cache:
    """Get the global cache instance.
    
    注意：此函数返回的是内存缓存实例，如需持久化存储，请使用 get_db_cache() 函数。
    """
    return _cache
