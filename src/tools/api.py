"""
API 功能模块，提供与金融数据API交互的功能

此模块整合了各个子模块的功能，提供统一的接口。
"""
from dotenv import load_dotenv
load_dotenv()

# 从各个子模块导入功能
from src.tools.api_base import (
    check_rate_limit,
    calculate_growth,
    ALPHA_VANTAGE_API_KEY,
    FIELD_MAPPING
)

from src.tools.api_cache import (
    get_cache_path,
    save_to_file_cache,
    load_from_file_cache,
    should_refresh_financial_data
)

from src.tools.api_models import (
    MetricsWrapper,
    CompanyNews
)

from src.tools.api_price import (
    get_prices,
    prices_to_df,
    get_price_data,
    get_market_cap
)

from src.tools.api_financials import (
    get_financial_metrics,
    search_line_items
)

from src.tools.api_insider import (
    get_insider_trades
)

from src.tools.api_news import (
    get_company_news
)

# 导出所有功能
__all__ = [
    # 基础功能
    'check_rate_limit',
    'calculate_growth',
    'ALPHA_VANTAGE_API_KEY',
    'FIELD_MAPPING',
    
    # 缓存功能
    'get_cache_path',
    'save_to_file_cache',
    'load_from_file_cache',
    'should_refresh_financial_data',
    
    # 数据模型
    'MetricsWrapper',
    'CompanyNews',
    
    # 价格相关API
    'get_prices',
    'prices_to_df',
    'get_price_data',
    'get_market_cap',
    
    # 财务指标相关API
    'get_financial_metrics',
    'search_line_items',
    
    # 内部交易相关API
    'get_insider_trades',
    
    # 公司新闻相关API
    'get_company_news'
]
