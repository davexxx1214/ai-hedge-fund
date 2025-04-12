import sqlite3
import json # Although not directly used in these methods now, keep for potential future needs

class DatabaseNewsMixin:
    """Mixin class for company news data operations."""

    def set_company_news(self, ticker: str, news_list: list):
        """
        存储公司新闻数据 (来自 CompanyNews 对象列表或类似结构的字典列表) 到数据库。
        使用 INSERT OR IGNORE 避免插入重复记录 (基于 ticker 和 url)。
        """
        if not self.conn:
            print("错误：数据库连接未建立，无法设置公司新闻数据。")
            return
        if not isinstance(news_list, list):
            print(f"错误：提供的公司新闻数据不是列表格式: {type(news_list)}")
            return
        if not news_list:
            # print(f"没有公司新闻数据需要存储 ({ticker})。") # Avoid excessive logging
            return

        cursor = self.conn.cursor()
        insert_sql = """
        INSERT OR IGNORE INTO company_news (
            ticker, url, date, time_published_raw, title, summary,
            sentiment_score, sentiment_label, author, topics,
            source_domain, banner_image, category_within_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        rows_to_insert = []
        # 数据库字段顺序，与 SQL 语句中的 VALUES 占位符对应
        db_fields_order = [
            'ticker', 'url', 'date', 'time_published_raw', 'title', 'summary',
            'sentiment_score', 'sentiment_label', 'author', 'topics',
            'source_domain', 'banner_image', 'category_within_source'
        ]
        # 映射 API/对象属性名到数据库字段名 (如果不同)
        # 注意：原始代码假设 news_item 是 CompanyNews 对象，并直接访问属性。
        # 这里我们尝试更通用地处理字典或对象。
        api_to_db_map = {
            'sentiment': 'sentiment_score',
            'overall_sentiment_label': 'sentiment_label',
            'time_published': 'time_published_raw' # 假设原始时间戳字段名为 time_published
            # 其他字段名假设一致
        }


        for news_item in news_list:
            # 尝试从对象或字典获取数据
            item_data = {}
            if hasattr(news_item, '__dict__'): # Standard object
                item_data = news_item.__dict__
            elif isinstance(news_item, dict): # Dictionary
                item_data = news_item
            else:
                 print(f"警告：跳过无法处理的公司新闻数据项格式: {type(news_item)}")
                 continue

            # 检查必需的 URL 字段
            url = item_data.get('url')
            if not url:
                title = item_data.get('title', 'N/A')
                print(f"警告：跳过缺少 URL 的新闻项 ({ticker}): {title}")
                continue

            # 构建要插入的行数据元组，按 db_fields_order 顺序
            row_tuple = []
            for db_field in db_fields_order:
                # 查找对应的 API/对象属性名
                api_key = next((k for k, v in api_to_db_map.items() if v == db_field), db_field)
                value = item_data.get(api_key)

                # 特殊处理 time_published_raw (如果映射存在)
                if db_field == 'time_published_raw' and 'time_published' in item_data:
                     value = item_data.get('time_published') # 优先使用原始字段名

                # 特殊处理 topics 和 author (可能是列表)
                if db_field in ['topics', 'author'] and isinstance(value, list):
                    try:
                        # 将列表转换为 JSON 字符串存储
                        value = json.dumps(value, ensure_ascii=False)
                    except TypeError:
                         print(f"警告：无法序列化字段 '{db_field}' 的列表值，将存为 NULL。")
                         value = None

                row_tuple.append(value)

            rows_to_insert.append(tuple(row_tuple))


        if rows_to_insert:
            try:
                cursor.executemany(insert_sql, rows_to_insert)
                self.conn.commit()
                # print(f"成功插入或忽略了 {len(rows_to_insert)} 条 {ticker} 的新闻记录。")
            except sqlite3.Error as e:
                print(f"批量插入 {ticker} 新闻数据时出错: {e}\nSQL: {insert_sql}\n示例数据: {rows_to_insert[0] if rows_to_insert else 'N/A'}")
                self.conn.rollback() # 出错时回滚
            except Exception as e:
                print(f"处理 {ticker} 新闻数据时发生意外错误: {e}")
                self.conn.rollback()

    def get_company_news(self, ticker, start_date=None, end_date=None):
        """获取公司新闻数据"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取公司新闻数据。")
            return []

        cursor = self.conn.cursor()
        sql = "SELECT * FROM company_news WHERE ticker = ?"
        params = [ticker]

        # 假设 'date' 字段是 YYYY-MM-DD 格式
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)

        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)

        sql += " ORDER BY date DESC, fetched_at DESC" # Order by published date, then fetch time

        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()

            # 转换为字典列表
            result = []
            for row in rows:
                item = dict(row)
                # 可选：将存储为 JSON 字符串的字段解析回列表
                for field in ['topics', 'author']:
                    if item.get(field) and isinstance(item[field], str):
                        try:
                            item[field] = json.loads(item[field])
                        except json.JSONDecodeError:
                            # print(f"警告：无法解析字段 '{field}' 的 JSON 数据: {item[field]}")
                            pass # 保留原始字符串或设为 None/[]
                result.append(item)

            return result
        except sqlite3.Error as e:
            print(f"获取公司新闻数据时出错 ({ticker}): {e}")
            return []
