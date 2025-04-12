import sqlite3
from pathlib import Path # Needed for get_database_stats

class DatabaseUtilsMixin:
    """Mixin class for database utility and helper methods."""

    def execute_query(self, sql, params=None):
        """执行自定义SQL查询 (SELECT)"""
        if not self.conn:
            print("错误：数据库连接未建立，无法执行查询。")
            return []

        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            rows = cursor.fetchall()
            # 转换为字典列表
            result = [dict(row) for row in rows]
            return result
        except sqlite3.Error as e:
            print(f"执行查询时出错: {e}\nSQL: {sql}\nParams: {params}")
            return []

    def execute_update(self, sql, params=None):
        """执行自定义SQL更新操作 (INSERT, UPDATE, DELETE)"""
        if not self.conn:
            print("错误：数据库连接未建立，无法执行更新。")
            return 0 # Return 0 rows affected

        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            self.conn.commit()
            return cursor.rowcount # 返回受影响的行数
        except sqlite3.Error as e:
            print(f"执行更新时出错: {e}\nSQL: {sql}\nParams: {params}")
            self.conn.rollback()
            return 0

    def get_table_schema(self, table_name):
        """获取表结构信息"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取表结构。")
            return []
        # Basic validation to prevent SQL injection if table_name comes from unsafe source
        if not table_name.isalnum() and '_' not in table_name:
             print(f"错误：无效的表名 '{table_name}'")
             return []

        cursor = self.conn.cursor()
        try:
            cursor.execute(f"PRAGMA table_info({table_name})")
            return cursor.fetchall() # Returns list of tuples
        except sqlite3.Error as e:
            print(f"获取表 '{table_name}' 结构时出错: {e}")
            return []

    def get_tables(self):
        """获取所有表名"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取表列表。")
            return []

        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return [row[0] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"获取表列表时出错: {e}")
            return []

    def get_table_count(self, table_name):
        """获取表中的记录数"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取表计数。")
            return 0
        # Basic validation
        if not table_name.isalnum() and '_' not in table_name:
             print(f"错误：无效的表名 '{table_name}'")
             return 0

        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return count if count is not None else 0
        except sqlite3.Error as e:
            # Handle case where table might not exist yet gracefully
            if "no such table" in str(e):
                # print(f"表 '{table_name}' 不存在，计数为 0。")
                return 0
            else:
                print(f"获取表 '{table_name}' 计数时出错: {e}")
                return 0 # Or raise error? Returning 0 seems safer.

    def get_ticker_stats(self, ticker):
        """获取指定股票的统计信息 (记录数和价格日期范围)"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取 Ticker 统计信息。")
            return {}

        stats = {}
        # 获取各表中该股票的记录数
        # Note: financial_metrics and line_items tables are not in the current schema.
        # Adjust the list based on actual tables where ticker is relevant.
        tables_with_ticker = [
            'company_overview', 'prices', 'income_statement_annual', 'balance_sheet_annual',
            'cash_flow_annual', 'income_statement_quarterly', 'balance_sheet_quarterly',
            'cash_flow_quarterly', 'insider_trades', 'company_news'
        ]
        cursor = self.conn.cursor()
        for table in tables_with_ticker:
            try:
                # Check if table exists before querying
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                if cursor.fetchone():
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE ticker = ?", [ticker])
                    count = cursor.fetchone()[0]
                    stats[f"{table}_count"] = count if count is not None else 0
                else:
                    stats[f"{table}_count"] = 0 # Table doesn't exist
            except sqlite3.Error as e:
                 print(f"获取表 '{table}' 中 Ticker '{ticker}' 计数时出错: {e}")
                 stats[f"{table}_count"] = 'Error' # Indicate error

        # 获取价格数据的日期范围
        try:
            cursor.execute("SELECT MIN(time), MAX(time) FROM prices WHERE ticker = ?", [ticker])
            date_range = cursor.fetchone()
            if date_range and date_range[0] is not None:
                stats['price_date_range'] = {'start': date_range[0], 'end': date_range[1]}
            else:
                 stats['price_date_range'] = None # No price data found
        except sqlite3.Error as e:
            print(f"获取 Ticker '{ticker}' 价格日期范围时出错: {e}")
            stats['price_date_range'] = 'Error'

        return stats

    def get_database_stats(self):
        """获取整个数据库的统计信息"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取数据库统计信息。")
            return {}

        stats = {'table_counts': {}}
        cursor = self.conn.cursor()

        # 获取各表的记录数
        tables = self.get_tables() # Use the method defined above
        for table in tables:
            stats['table_counts'][table] = self.get_table_count(table) # Use the method defined above

        # 获取所有不同的股票代码 (从一个代表性的表，如 prices or company_overview)
        distinct_tickers = set()
        try:
            cursor.execute("SELECT DISTINCT ticker FROM company_overview")
            distinct_tickers.update(row[0] for row in cursor.fetchall() if row[0])
        except sqlite3.Error:
             print("无法从 company_overview 获取 Tickers，尝试 prices...")
             try:
                 cursor.execute("SELECT DISTINCT ticker FROM prices")
                 distinct_tickers.update(row[0] for row in cursor.fetchall() if row[0])
             except sqlite3.Error as e:
                 print(f"获取不同 Ticker 列表时出错: {e}")

        stats['distinct_tickers'] = sorted(list(distinct_tickers))
        stats['distinct_ticker_count'] = len(distinct_tickers)

        # 获取数据库文件大小
        try:
            if self.db_path.exists():
                stats['db_size_bytes'] = self.db_path.stat().st_size
            else:
                stats['db_size_bytes'] = 0
        except Exception as e:
            print(f"获取数据库文件大小时出错: {e}")
            stats['db_size_bytes'] = 'Error'

        return stats
