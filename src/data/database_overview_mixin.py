import sqlite3
from datetime import datetime

class DatabaseOverviewMixin:
    """Mixin class for company overview data operations."""

    def set_company_overview(self, ticker, data):
        """存储公司概览数据"""
        if not self.conn:
            print("错误：数据库连接未建立，无法设置公司概览。")
            return False
        if not isinstance(data, dict):
            print(f"错误：提供的公司概览数据不是字典格式: {type(data)}")
            return False

        cursor = self.conn.cursor()

        # 准备数据
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 字段映射，将API返回的字段名转换为数据库字段名
        field_mapping = {
            'Symbol': 'symbol', 'AssetType': 'asset_type', 'Name': 'name',
            'Description': 'description', 'CIK': 'cik', 'Exchange': 'exchange',
            'Currency': 'currency', 'Country': 'country', 'Sector': 'sector',
            'Industry': 'industry', 'Address': 'address', 'OfficialSite': 'official_site',
            'FiscalYearEnd': 'fiscal_year_end', 'LatestQuarter': 'latest_quarter',
            'MarketCapitalization': 'market_capitalization', 'EBITDA': 'ebitda',
            'PERatio': 'pe_ratio', 'PEGRatio': 'peg_ratio', 'BookValue': 'book_value',
            'DividendPerShare': 'dividend_per_share', 'DividendYield': 'dividend_yield',
            'EPS': 'eps', 'RevenuePerShareTTM': 'revenue_per_share_ttm',
            'ProfitMargin': 'profit_margin', 'OperatingMarginTTM': 'operating_margin_ttm',
            'ReturnOnAssetsTTM': 'return_on_assets_ttm', 'ReturnOnEquityTTM': 'return_on_equity_ttm',
            'RevenueTTM': 'revenue_ttm', 'GrossProfitTTM': 'gross_profit_ttm',
            'DilutedEPSTTM': 'diluted_eps_ttm',
            'QuarterlyEarningsGrowthYOY': 'quarterly_earnings_growth_yoy',
            'QuarterlyRevenueGrowthYOY': 'quarterly_revenue_growth_yoy',
            'AnalystTargetPrice': 'analyst_target_price',
            'AnalystRatingStrongBuy': 'analyst_rating_strong_buy',
            'AnalystRatingBuy': 'analyst_rating_buy', 'AnalystRatingHold': 'analyst_rating_hold',
            'AnalystRatingSell': 'analyst_rating_sell', 'AnalystRatingStrongSell': 'analyst_rating_strong_sell',
            'TrailingPE': 'trailing_pe', 'ForwardPE': 'forward_pe',
            'PriceToSalesRatioTTM': 'price_to_sales_ratio_ttm', 'PriceToBookRatio': 'price_to_book_ratio',
            'EVToRevenue': 'ev_to_revenue', 'EVToEBITDA': 'ev_to_ebitda', 'Beta': 'beta',
            '52WeekHigh': 'week_52_high', '52WeekLow': 'week_52_low',
            '50DayMovingAverage': 'day_50_moving_average', '200DayMovingAverage': 'day_200_moving_average',
            'SharesOutstanding': 'shares_outstanding', 'DividendDate': 'dividend_date',
            'ExDividendDate': 'ex_dividend_date'
        }

        # 数值型字段，需要尝试转换为浮点数
        numeric_fields_api = {
            'MarketCapitalization', 'EBITDA', 'PERatio', 'PEGRatio', 'BookValue',
            'DividendPerShare', 'DividendYield', 'EPS', 'RevenuePerShareTTM',
            'ProfitMargin', 'OperatingMarginTTM', 'ReturnOnAssetsTTM', 'ReturnOnEquityTTM',
            'RevenueTTM', 'GrossProfitTTM', 'DilutedEPSTTM', 'QuarterlyEarningsGrowthYOY',
            'QuarterlyRevenueGrowthYOY', 'AnalystTargetPrice', 'AnalystRatingStrongBuy',
            'AnalystRatingBuy', 'AnalystRatingHold', 'AnalystRatingSell',
            'AnalystRatingStrongSell', 'TrailingPE', 'ForwardPE', 'PriceToSalesRatioTTM',
            'PriceToBookRatio', 'EVToRevenue', 'EVToEBITDA', 'Beta', '52WeekHigh',
            '52WeekLow', '50DayMovingAverage', '200DayMovingAverage', 'SharesOutstanding'
        }

        # 准备插入的字段和值
        fields = ['ticker', 'last_updated']
        values = [ticker, now]

        # 动态添加其他字段
        for api_field, db_field in field_mapping.items():
            raw_value = data.get(api_field) # Use .get() for safety

            # --- Start Fix: Handle potential nested dict {None: value} ---
            if isinstance(raw_value, dict) and None in raw_value:
                value = raw_value[None]
            else:
                value = raw_value
            # --- End Fix ---

            if value is not None and value != 'None': # Check for None and 'None' string
                fields.append(db_field)
                # 尝试将数值字段转换为浮点数
                if api_field in numeric_fields_api:
                    try:
                        # Handle potential percentage signs or other non-numeric chars if necessary
                        if isinstance(value, str):
                            value = value.replace('%', '')
                        values.append(float(value))
                    except (ValueError, TypeError):
                        # print(f"警告：无法将字段 '{api_field}' 的值 '{value}' 转换为浮点数，将存为 NULL。")
                        values.append(None) # Store as NULL if conversion fails
                else:
                    values.append(value) # Append non-numeric or already converted value

        # 构建SQL语句
        placeholders = ', '.join(['?'] * len(fields))
        fields_str = ', '.join(fields)

        # 使用INSERT OR REPLACE确保唯一性
        sql = f"INSERT OR REPLACE INTO company_overview ({fields_str}) VALUES ({placeholders})"

        try:
            cursor.execute(sql, values)
            self.conn.commit()
            # print(f"公司概览数据已存储: {ticker}") # Optional success log
            return True
        except sqlite3.Error as e:
            print(f"存储公司概览数据时出错 ({ticker}): {e}\nSQL: {sql}\nValues: {values}")
            self.conn.rollback()
            return False
        except Exception as e:
            print(f"处理公司概览数据时发生意外错误 ({ticker}): {e}")
            self.conn.rollback()
            return False

    def get_company_overview(self, ticker):
        """获取公司概览数据"""
        if not self.conn:
            print("错误：数据库连接未建立，无法获取公司概览。")
            return None

        cursor = self.conn.cursor()
        sql = "SELECT * FROM company_overview WHERE ticker = ?"
        params = [ticker]

        try:
            cursor.execute(sql, params)
            row = cursor.fetchone()

            if row:
                return dict(row)
            else:
                return None
        except sqlite3.Error as e:
            print(f"获取公司概览数据时出错 ({ticker}): {e}")
            return None
