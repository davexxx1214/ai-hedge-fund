"""
数据模型类定义
"""

class MetricsWrapper:
    """
    用于包装财务指标数据，使之支持 model_dump() 方法（类似 pydantic 对象）
    """
    def __init__(self, data: dict):
        self.__dict__.update(data)
    def model_dump(self):
        return self.__dict__

class CompanyNews:
    """
    封装 Alpha Vantage 新闻数据，使新闻项支持属性访问。
    映射字段说明：
      - time_published 映射为 date（仅保留日期部分）
      - overall_sentiment_score 映射为 sentiment（转换为 float 类型）
      - overall_sentiment_label 保持原样（如"Somewhat-Bullish"）
      - title 确保存在，默认为空字符串
      - authors 列表转换为字符串，用逗号分隔
      - topics 列表转换为字符串，用逗号分隔
    例如，可以使用 news.sentiment 访问新闻情感数据，使用 news.date 进行日期过滤。
    """
    def __init__(self, **kwargs):
        # 将 time_published 映射为 date（只取日期部分，"T"或空格分隔）
        tp = kwargs.get("time_published", "")
        if tp:
            if "T" in tp:
                self.date = tp.split("T")[0]
            else:
                self.date = tp.split(" ")[0]
        else:
            # 如果没有time_published，则尝试使用date字段
            date_str = kwargs.get("date", "")
            # 如果date字段是格式为"YYYYMMDD"的字符串，则转换为"YYYY-MM-DD"格式
            if date_str and len(date_str) == 8 and date_str.isdigit():
                self.date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                self.date = date_str
        
        # 将 overall_sentiment_score 映射为 sentiment，并转换为 float 类型（若无法转换则为 None）
        s = kwargs.get("overall_sentiment_score", None)
        try:
            self.sentiment = float(s) if s is not None else None
        except Exception:
            # 如果overall_sentiment_score转换失败，尝试使用sentiment字段
            try:
                self.sentiment = float(kwargs.get("sentiment", 0)) if kwargs.get("sentiment") is not None else None
            except Exception:
                self.sentiment = None
        
        # 处理 overall_sentiment_label 字段
        self.overall_sentiment_label = kwargs.get("overall_sentiment_label", "")
        
        # 确保title属性存在
        self.title = kwargs.get("title", "")
        
        # 处理authors字段，如果是列表则转换为字符串
        authors = kwargs.get("authors", [])
        if isinstance(authors, list):
            self.author = ", ".join(authors)  # 使用author字段存储，而不是authors
        else:
            self.author = str(authors)
        
        # 处理summary字段
        self.summary = kwargs.get("summary", "")
        
        # 处理banner_image字段
        self.banner_image = kwargs.get("banner_image", "")
        
        # 处理source_domain字段
        self.source_domain = kwargs.get("source_domain", "")
        
        # 处理category_within_source字段
        self.category_within_source = kwargs.get("category_within_source", "")
        
        # 处理topics字段，如果是列表则转换为字符串
        topics = kwargs.get("topics", [])
        if isinstance(topics, list):
            # 如果topics是包含字典的列表，提取topic字段
            if topics and isinstance(topics[0], dict) and "topic" in topics[0]:
                self.topics = ", ".join([t.get("topic", "") for t in topics])
            else:
                self.topics = ", ".join([str(t) for t in topics])
        else:
            self.topics = str(topics)
        
        # 将其他属性也添加进来，但不覆盖已有的特殊处理字段
        skip_fields = ["time_published", "overall_sentiment_score", "overall_sentiment_label", "title", "authors", 
                      "summary", "banner_image", "source_domain", "category_within_source", "topics", "ticker_sentiment"]
        temp = {k: v for k, v in kwargs.items() if k not in skip_fields}
        self.__dict__.update(temp)
    
    def model_dump(self):
        # 确保返回的字典不包含可能导致问题的字段
        data = self.__dict__.copy()
        # 删除可能导致问题的字段
        for field in ["authors", "topics", "ticker_sentiment"]:
            if field in data:
                del data[field]
        return data
