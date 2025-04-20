import sqlite3
from datetime import datetime

class DatabaseInsiderMixin:
    """Mixin class for insider trading data operations."""

    def set_insider_trades(self, ticker, data):
        """存储内部交易数据 (data should be a list of dicts or objects)"""
        if not self.conn:
            print("错误：数据库连接未建立，无法设置内部交易数据。")
            return
        if not isinstance(data, list):
            print(f"错误：提供的内部交易数据不是列表格式: {type(data)}")
            return

        cursor = self.conn.cursor()
        rows_to_insert = []
        # 数据库字段
        db_fields_set = {
            'ticker', 'issuer', 'name', 'title', 'is_board_director',
            'transaction_date', 'transaction_shares', 'transaction_price_per_share',
            'transaction_value', 'shares_owned_before_transaction',
            'shares_owned_after_transaction', 'security_title', 'filing_date'
        }
        # API/输入数据到数据库字段的映射
        api_to_db_map = {
            'date': 'transaction_date',
            'insider_name': 'name',
            'insider_title': 'title',
            'price': 'transaction_price_per_share',
            'value': 'transaction_value',
            'shares_owned': 'shares_owned_after_transaction'
            # 其他字段名如果与数据库一致，则无需映射
        }

        for item in data:
            # 获取item的数据，支持字典和对象两种情况
            if hasattr(item, 'model_dump'): # Pydantic v2+
                item_data = item.model_dump()
            elif hasattr(item, '__dict__'): # Standard object
                item_data = item.__dict__
            elif isinstance(item, dict): # Dictionary
                item_data = item
            else:
                print(f"警告：跳过无法处理的内部交易数据项格式: {type(item)}")
                continue

            # 准备插入的字段和值
            current_fields = ['ticker']
            current_values = [ticker]
            processed_keys = set()

            # 动态添加其他字段，应用映射
            for key, value in item_data.items():
                db_key = api_to_db_map.get(key, key) # Apply mapping, default to original key
                if db_key in db_fields_set:
                    current_fields.append(db_key)
                    current_values.append(value)
                    processed_keys.add(db_key)

            # 确保关键日期字段存在，如果缺少则使用当前日期
            if 'transaction_date' not in processed_keys:
                current_fields.append('transaction_date')
                current_values.append(datetime.now().strftime('%Y-%m-%d'))
                print(f"警告：内部交易数据缺少 'transaction_date'，使用当前日期。数据: {item_data}")
            if 'filing_date' not in processed_keys:
                current_fields.append('filing_date')
                current_values.append(datetime.now().strftime('%Y-%m-%d'))
                print(f"警告：内部交易数据缺少 'filing_date'，使用当前日期。数据: {item_data}")

            # 确保所有数据库字段都有对应的值（即使是None），以匹配SQL语句
            value_map = dict(zip(current_fields, current_values))
            # 只有在有有效数据时才插入（例如，存在name）
            name_value = value_map.get('name')
            if name_value:
                final_values = []
                all_db_fields_ordered = sorted(list(db_fields_set)) # Get a consistent order
                for field in all_db_fields_ordered:
                    final_values.append(value_map.get(field)) # Append value or None if missing

                rows_to_insert.append(tuple(final_values))
            else:
                print(f"跳过无效内部交易数据: {item_data}")

        if not rows_to_insert:
            print(f"没有有效的内部交易数据可供插入 ({ticker})。")
            return

        # 构建SQL语句 (使用固定的、排序的字段列表)
        all_db_fields_ordered_str = ', '.join(all_db_fields_ordered)
        placeholders = ', '.join(['?'] * len(all_db_fields_ordered))
        sql = f"INSERT OR REPLACE INTO insider_trades ({all_db_fields_ordered_str}) VALUES ({placeholders})"

        try:
            cursor.executemany(sql, rows_to_insert)
            self.conn.commit()
            # print(f"内部交易数据已存储: {ticker}, 记录数: {len(rows_to_insert)}")
        except sqlite3.Error as e:
            print(f"批量存储内部交易数据时出错 ({ticker}): {e}\nSQL: {sql}\n示例数据: {rows_to_insert[0] if rows_to_insert else 'N/A'}")
            self.conn.rollback()
        except Exception as e:
            print(f"处理内部交易数据时发生意外错误 ({ticker}): {e}")
            self.conn.rollback()

    def get_insider_trades(self, ticker, start_date=None, end_date=None):
        """获取内部交易数据"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取内部交易数据。")
            return []

        cursor = self.conn.cursor()
        sql = "SELECT * FROM insider_trades WHERE ticker = ?"
        params = [ticker]

        if start_date:
            # Assuming transaction_date is the relevant date field
            sql += " AND transaction_date >= ?"
            params.append(start_date)

        if end_date:
            sql += " AND transaction_date <= ?"
            params.append(end_date)

        sql += " ORDER BY transaction_date DESC, filing_date DESC" # Order by transaction, then filing

        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()

            # 转换为字典列表，并映射回原始字段名
            result = []
            db_to_api_map = {
                'transaction_date': 'date',
                'name': 'insider_name',
                'title': 'insider_title',
                'transaction_price_per_share': 'price',
                'transaction_value': 'value',
                'transaction_shares': 'transaction_shares',
                'shares_owned_after_transaction': 'shares_owned'
            }

            for row in rows:
                item = dict(row)
                mapped_item = {}
                for db_key, value in item.items():
                    api_key = db_to_api_map.get(db_key, db_key)
                    mapped_item[api_key] = value
                result.append(mapped_item)

            return result
        except sqlite3.Error as e:
            print(f"获取内部交易数据时出错 ({ticker}): {e}")
            return []
