import sqlite3

class DatabasePricesMixin:
    """Mixin class for stock price data operations."""

    def set_prices(self, ticker, data):
        """存储价格数据 (data should be a list of dicts)"""
        if not self.conn:
            print("错误：数据库连接未建立，无法设置价格数据。")
            return
        if not isinstance(data, list):
            print(f"错误：提供的价格数据不是列表格式: {type(data)}")
            return

        cursor = self.conn.cursor()
        rows_to_insert = []
        allowed_fields = {'time', 'open', 'close', 'high', 'low', 'volume',
                          'adjusted_close', 'dividend_amount', 'split_coefficient'}

        for item in data:
            if not isinstance(item, dict):
                print(f"警告：跳过非字典格式的价格数据项: {item}")
                continue
            if 'time' not in item or item['time'] is None:
                print(f"警告：跳过缺少 'time' 字段的价格数据项: {item}")
                continue

            # 准备插入的字段和值
            fields = ['ticker']
            values = [ticker]
            valid_item = True
            for key, value in item.items():
                if key in allowed_fields:
                    # 尝试转换数值类型，如果失败则记录为 None
                    if key in ['open', 'close', 'high', 'low', 'adjusted_close', 'dividend_amount', 'split_coefficient']:
                        try:
                            values.append(float(value) if value is not None else None)
                        except (ValueError, TypeError):
                            values.append(None)
                    elif key == 'volume':
                        try:
                            values.append(int(value) if value is not None else None)
                        except (ValueError, TypeError):
                            values.append(None)
                    else: # time field
                         values.append(value)
                    fields.append(key)
                # else: # Ignore unexpected fields silently or log warning
                #     print(f"警告: 价格数据中发现未知字段 '{key}'，已忽略。")

            if valid_item:
                 rows_to_insert.append(tuple(values)) # Ensure values are in correct order matching fields

        if not rows_to_insert:
            print(f"没有有效的价格数据可供插入 ({ticker})。")
            return

        # 构建SQL语句 (一次性构建)
        # Assuming all valid rows have the same fields in the same order after filtering
        first_row_fields = ['ticker'] + [k for k in data[0].keys() if k in allowed_fields and k != 'time']
        first_row_fields.insert(1, 'time') # Ensure time is second field
        placeholders = ', '.join(['?'] * len(first_row_fields))
        fields_str = ', '.join(first_row_fields)
        sql = f"INSERT OR REPLACE INTO prices ({fields_str}) VALUES ({placeholders})"

        try:
            cursor.executemany(sql, rows_to_insert)
            self.conn.commit()
            # print(f"价格数据已存储: {ticker}, 记录数: {len(rows_to_insert)}") # Optional success log
        except sqlite3.Error as e:
            print(f"批量存储价格数据时出错 ({ticker}): {e}\nSQL: {sql}\n示例数据: {rows_to_insert[0] if rows_to_insert else 'N/A'}")
            self.conn.rollback()
        except Exception as e:
             print(f"处理价格数据时发生意外错误 ({ticker}): {e}")
             self.conn.rollback()


    def get_prices(self, ticker, start_date=None, end_date=None):
        """获取价格数据"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取价格数据。")
            return []

        cursor = self.conn.cursor()
        sql = "SELECT * FROM prices WHERE ticker = ?"
        params = [ticker]

        if start_date:
            sql += " AND time >= ?"
            params.append(start_date)

        if end_date:
            sql += " AND time <= ?"
            params.append(end_date)

        sql += " ORDER BY time ASC" # Ensure chronological order

        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()

            # 转换为字典列表
            result = [dict(row) for row in rows]
            return result
        except sqlite3.Error as e:
            print(f"获取价格数据时出错 ({ticker}): {e}")
            return []
