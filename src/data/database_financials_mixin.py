import sqlite3
import json

class DatabaseFinancialsMixin:
    """Mixin class for financial statement data operations (annual and quarterly)."""

    # --- Helper for inserting financial data ---
    def _set_financial_data(self, table_name, ticker, data_list, allowed_fields=None):
        """通用函数，用于存储财务报表数据 (年报/季报)"""
        if not self.conn:
            print(f"错误：数据库连接未建立，无法设置 {table_name} 数据。")
            return
        if not isinstance(data_list, list):
            print(f"错误：提供的 {table_name} 数据不是列表格式: {type(data_list)}")
            return

        cursor = self.conn.cursor()
        rows_to_insert = []

        for item in data_list:
            if not isinstance(item, dict):
                print(f"警告：跳过非字典格式的 {table_name} 数据项: {item}")
                continue
            if 'fiscalDateEnding' not in item or item['fiscalDateEnding'] is None:
                print(f"警告：跳过缺少 'fiscalDateEnding' 字段的 {table_name} 数据项: {item}")
                continue

            # 准备插入的字段和值
            fields = ['ticker']
            values = [ticker]
            valid_item = True
            for key, value in item.items():
                # 如果提供了允许的字段集，则进行过滤
                if allowed_fields and key not in allowed_fields:
                    # print(f"警告: {table_name} 数据中发现未知或不允许的字段 '{key}'，已忽略。")
                    continue
                # 复杂类型转字符串 (例如，如果API返回嵌套结构)
                if isinstance(value, (dict, list)):
                    try:
                        value = json.dumps(value, ensure_ascii=False)
                    except TypeError:
                        print(f"警告：无法序列化字段 '{key}' 的值，将存为 NULL。")
                        value = None
                # 处理 'None' 字符串和 None 值
                if value == 'None':
                    value = None

                fields.append(key)
                values.append(value)

            if valid_item:
                rows_to_insert.append(tuple(values))

        if not rows_to_insert:
            print(f"没有有效的 {table_name} 数据可供插入 ({ticker})。")
            return

        # 构建SQL语句 (基于第一个有效数据项的字段)
        # 假设所有有效行具有相似的结构
        first_valid_item_keys = [k for k in data_list[0].keys() if (not allowed_fields or k in allowed_fields)]
        db_fields = ['ticker'] + first_valid_item_keys
        placeholders = ', '.join(['?'] * len(db_fields))
        fields_str = ', '.join(db_fields)
        sql = f"INSERT OR REPLACE INTO {table_name} ({fields_str}) VALUES ({placeholders})"

        try:
            cursor.executemany(sql, rows_to_insert)
            self.conn.commit()
            # print(f"{table_name} 数据已存储: {ticker}, 记录数: {len(rows_to_insert)}")
        except sqlite3.Error as e:
            print(f"批量存储 {table_name} 数据时出错 ({ticker}): {e}\nSQL: {sql}\n示例数据: {rows_to_insert[0] if rows_to_insert else 'N/A'}")
            self.conn.rollback()
        except Exception as e:
            print(f"处理 {table_name} 数据时发生意外错误 ({ticker}): {e}")
            self.conn.rollback()

    # --- Helper for getting financial data ---
    def _get_financial_data(self, table_name, ticker, fiscal_date_ending=None):
        """通用函数，用于获取财务报表数据 (年报/季报)"""
        if not self.conn:
            print(f"错误：数据库连接未建立，无法获取 {table_name} 数据。")
            return []

        cursor = self.conn.cursor()
        sql = f"SELECT * FROM {table_name} WHERE ticker = ?"
        params = [ticker]

        if fiscal_date_ending:
            sql += " AND fiscalDateEnding = ?"
            params.append(fiscal_date_ending)

        sql += " ORDER BY fiscalDateEnding DESC"

        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            # 转换为字典列表
            result = [dict(row) for row in rows]
            return result
        except sqlite3.Error as e:
            print(f"获取 {table_name} 数据时出错 ({ticker}): {e}")
            return []

    # --- Income Statement Annual ---
    def set_income_statement_annual(self, ticker, data):
        """存储年度利润表数据"""
        # 定义此表允许的字段，以增加健壮性
        allowed = {
            'fiscalDateEnding', 'reportedCurrency', 'grossProfit', 'totalRevenue', 'costOfRevenue',
            'costofGoodsAndServicesSold', 'operatingIncome', 'sellingGeneralAndAdministrative',
            'researchAndDevelopment', 'operatingExpenses', 'investmentIncomeNet', 'netInterestIncome',
            'interestIncome', 'interestExpense', 'nonInterestIncome', 'otherNonOperatingIncome',
            'depreciation', 'depreciationAndAmortization', 'incomeBeforeTax', 'incomeTaxExpense',
            'interestAndDebtExpense', 'netIncomeFromContinuingOperations', 'comprehensiveIncomeNetOfTax',
            'ebit', 'ebitda', 'netIncome'
        }
        self._set_financial_data('income_statement_annual', ticker, data, allowed_fields=allowed)

    def get_income_statement_annual(self, ticker, fiscal_date_ending=None):
        """获取年度利润表数据"""
        return self._get_financial_data('income_statement_annual', ticker, fiscal_date_ending)

    # --- Balance Sheet Annual ---
    def set_balance_sheet_annual(self, ticker, data):
        """存储年度资产负债表数据"""
        # 定义允许的字段 (可选，但推荐)
        allowed = {
            'fiscalDateEnding', 'reportedCurrency', 'totalAssets', 'totalCurrentAssets',
            'cashAndCashEquivalentsAtCarryingValue', 'cashAndShortTermInvestments', 'inventory',
            'currentNetReceivables', 'totalNonCurrentAssets', 'propertyPlantEquipment',
            'accumulatedDepreciationAmortizationPPE', 'intangibleAssets',
            'intangibleAssetsExcludingGoodwill', 'goodwill', 'investments', 'longTermInvestments',
            'shortTermInvestments', 'otherCurrentAssets', 'otherNonCurrentAssets', 'totalLiabilities',
            'totalCurrentLiabilities', 'currentAccountsPayable', 'deferredRevenue', 'currentDebt',
            'shortTermDebt', 'totalNonCurrentLiabilities', 'capitalLeaseObligations', 'longTermDebt',
            'currentLongTermDebt', 'longTermDebtNoncurrent', 'shortLongTermDebtTotal',
            'otherCurrentLiabilities', 'otherNonCurrentLiabilities', 'totalShareholderEquity',
            'treasuryStock', 'retainedEarnings', 'commonStock', 'commonStockSharesOutstanding'
        }
        self._set_financial_data('balance_sheet_annual', ticker, data, allowed_fields=allowed)

    def get_balance_sheet_annual(self, ticker, fiscal_date_ending=None):
        """获取年度资产负债表数据"""
        return self._get_financial_data('balance_sheet_annual', ticker, fiscal_date_ending)

    # --- Cash Flow Annual ---
    def set_cash_flow_annual(self, ticker, data):
        """存储年度现金流量表数据"""
        allowed = {
            'fiscalDateEnding', 'reportedCurrency', 'operatingCashflow', 'paymentsForOperatingActivities',
            'proceedsFromOperatingActivities', 'changeInOperatingLiabilities', 'changeInOperatingAssets',
            'depreciationDepletionAndAmortization', 'capitalExpenditures', 'changeInReceivables',
            'changeInInventory', 'profitLoss', 'cashflowFromInvestment', 'cashflowFromFinancing',
            'proceedsFromRepaymentsOfShortTermDebt', 'paymentsForRepurchaseOfCommonStock',
            'paymentsForRepurchaseOfEquity', 'paymentsForRepurchaseOfPreferredStock', 'dividendPayout',
            'dividendPayoutCommonStock', 'dividendPayoutPreferredStock', 'proceedsFromIssuanceOfCommonStock',
            'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet', 'proceedsFromIssuanceOfPreferredStock',
            'proceedsFromRepurchaseOfEquity', 'proceedsFromSaleOfTreasuryStock',
            'changeInCashAndCashEquivalents', 'changeInExchangeRate', 'netIncome'
        }
        self._set_financial_data('cash_flow_annual', ticker, data, allowed_fields=allowed)

    def get_cash_flow_annual(self, ticker, fiscal_date_ending=None):
        """获取年度现金流量表数据"""
        return self._get_financial_data('cash_flow_annual', ticker, fiscal_date_ending)

    # --- Income Statement Quarterly ---
    def set_income_statement_quarterly(self, ticker, data):
        """存储季度利润表数据"""
        # 使用与年度相同的允许字段集
        allowed = {
            'fiscalDateEnding', 'reportedCurrency', 'grossProfit', 'totalRevenue', 'costOfRevenue',
            'costofGoodsAndServicesSold', 'operatingIncome', 'sellingGeneralAndAdministrative',
            'researchAndDevelopment', 'operatingExpenses', 'investmentIncomeNet', 'netInterestIncome',
            'interestIncome', 'interestExpense', 'nonInterestIncome', 'otherNonOperatingIncome',
            'depreciation', 'depreciationAndAmortization', 'incomeBeforeTax', 'incomeTaxExpense',
            'interestAndDebtExpense', 'netIncomeFromContinuingOperations', 'comprehensiveIncomeNetOfTax',
            'ebit', 'ebitda', 'netIncome'
        }
        self._set_financial_data('income_statement_quarterly', ticker, data, allowed_fields=allowed)

    def get_income_statement_quarterly(self, ticker, fiscal_date_ending=None):
        """获取季度利润表数据"""
        return self._get_financial_data('income_statement_quarterly', ticker, fiscal_date_ending)

    # --- Balance Sheet Quarterly ---
    def set_balance_sheet_quarterly(self, ticker, data):
        """存储季度资产负债表数据"""
        # 使用与年度相同的允许字段集
        allowed = {
            'fiscalDateEnding', 'reportedCurrency', 'totalAssets', 'totalCurrentAssets',
            'cashAndCashEquivalentsAtCarryingValue', 'cashAndShortTermInvestments', 'inventory',
            'currentNetReceivables', 'totalNonCurrentAssets', 'propertyPlantEquipment',
            'accumulatedDepreciationAmortizationPPE', 'intangibleAssets',
            'intangibleAssetsExcludingGoodwill', 'goodwill', 'investments', 'longTermInvestments',
            'shortTermInvestments', 'otherCurrentAssets', 'otherNonCurrentAssets', 'totalLiabilities',
            'totalCurrentLiabilities', 'currentAccountsPayable', 'deferredRevenue', 'currentDebt',
            'shortTermDebt', 'totalNonCurrentLiabilities', 'capitalLeaseObligations', 'longTermDebt',
            'currentLongTermDebt', 'longTermDebtNoncurrent', 'shortLongTermDebtTotal',
            'otherCurrentLiabilities', 'otherNonCurrentLiabilities', 'totalShareholderEquity',
            'treasuryStock', 'retainedEarnings', 'commonStock', 'commonStockSharesOutstanding'
        }
        self._set_financial_data('balance_sheet_quarterly', ticker, data, allowed_fields=allowed)

    def get_balance_sheet_quarterly(self, ticker, fiscal_date_ending=None):
        """获取季度资产负债表数据"""
        return self._get_financial_data('balance_sheet_quarterly', ticker, fiscal_date_ending)

    # --- Cash Flow Quarterly ---
    def set_cash_flow_quarterly(self, ticker, data):
        """存储季度现金流量表数据"""
        # 使用与年度相同的允许字段集
        allowed = {
            'fiscalDateEnding', 'reportedCurrency', 'operatingCashflow', 'paymentsForOperatingActivities',
            'proceedsFromOperatingActivities', 'changeInOperatingLiabilities', 'changeInOperatingAssets',
            'depreciationDepletionAndAmortization', 'capitalExpenditures', 'changeInReceivables',
            'changeInInventory', 'profitLoss', 'cashflowFromInvestment', 'cashflowFromFinancing',
            'proceedsFromRepaymentsOfShortTermDebt', 'paymentsForRepurchaseOfCommonStock',
            'paymentsForRepurchaseOfEquity', 'paymentsForRepurchaseOfPreferredStock', 'dividendPayout',
            'dividendPayoutCommonStock', 'dividendPayoutPreferredStock', 'proceedsFromIssuanceOfCommonStock',
            'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet', 'proceedsFromIssuanceOfPreferredStock',
            'proceedsFromRepurchaseOfEquity', 'proceedsFromSaleOfTreasuryStock',
            'changeInCashAndCashEquivalents', 'changeInExchangeRate', 'netIncome'
        }
        self._set_financial_data('cash_flow_quarterly', ticker, data, allowed_fields=allowed)

    def get_cash_flow_quarterly(self, ticker, fiscal_date_ending=None):
        """获取季度现金流量表数据"""
        return self._get_financial_data('cash_flow_quarterly', ticker, fiscal_date_ending)
