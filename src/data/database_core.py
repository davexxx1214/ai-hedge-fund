import sqlite3
import threading
from pathlib import Path
from datetime import datetime
import json
import pandas as pd # Keep pandas import for potential future use or if needed by mixins indirectly
import os

# Import Mixin classes (will be created in subsequent steps)
from .database_tables import DatabaseTablesMixin
from .database_overview_mixin import DatabaseOverviewMixin
from .database_prices_mixin import DatabasePricesMixin
from .database_financials_mixin import DatabaseFinancialsMixin
from .database_insider_mixin import DatabaseInsiderMixin
from .database_news_mixin import DatabaseNewsMixin
from .database_utils_mixin import DatabaseUtilsMixin

# 使用线程本地存储
_thread_local = threading.local()

# 数据库文件路径 (Centralized definition)
DB_PATH = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/finance.db')))

class Database(
    DatabaseTablesMixin,
    DatabaseOverviewMixin,
    DatabasePricesMixin,
    DatabaseFinancialsMixin,
    DatabaseInsiderMixin,
    DatabaseNewsMixin,
    DatabaseUtilsMixin
):
    """SQLite数据库管理类，用于存储股票金融数据 (Core Structure)"""

    def __init__(self, db_path=DB_PATH):
        """初始化数据库连接"""
        self.db_path = db_path
        self.conn = None
        self._connect()
        # _create_tables and _update_table_structure will be called from DatabaseTablesMixin
        self._create_tables()
        self._update_table_structure() # Ensure updates are applied on init

    def _connect(self):
        """连接到数据库"""
        # Ensure the directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        print(f"数据库连接已建立: {self.db_path}")

    def close(self):
        """关闭数据库连接"""
        try:
            if self.conn:
                db_path_str = str(self.db_path) # Store path before closing
                self.conn.close()
                self.conn = None
                print(f"数据库连接已关闭: {db_path_str}")
        except Exception as e:
            # Log potential errors during close, but don't crash
            print(f"关闭数据库连接时发生错误 ({self.db_path}): {e}")
            pass # Ignore errors during close as per original logic

    def __del__(self):
        """析构函数，确保数据库连接被关闭"""
        try:
            # Check if connection exists and is not already closed
            if hasattr(self, 'conn') and self.conn:
                self.close()
        except Exception:
            # 忽略析构函数中的错误
            pass

def get_db():
    """获取数据库连接，确保每个线程使用自己的连接"""
    if not hasattr(_thread_local, 'db') or _thread_local.db.conn is None:
        # 为当前线程创建新的数据库连接
        # Use the centralized DB_PATH
        _thread_local.db = Database(DB_PATH)
        print(f"为线程 {threading.get_ident()} 创建/重新创建数据库连接")
    return _thread_local.db

# Example of how to use (for testing if run directly, though not typical)
if __name__ == '__main__':
    db_instance = get_db()
    print("数据库实例:", db_instance)
    print("表:", db_instance.get_tables())
    db_instance.close()
    print("数据库连接已关闭。")
