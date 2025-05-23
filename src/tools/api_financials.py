"""
财务指标相关API功能
"""
import pandas as pd
from datetime import datetime
import time

from src.tools.api_base import fd, check_rate_limit, calculate_growth, FIELD_MAPPING
from src.tools.api_cache import save_to_file_cache, should_refresh_financial_data
from src.tools.api_models import MetricsWrapper
from src.data.db_cache import get_db_cache
from src.data.database_core import get_db
from src.data.cache import get_cache

# 内存缓存实例
cache = get_cache()

def get_financial_metrics(ticker: str, end_date: str = None, period: str = "ttm", limit: int = 10) -> list:
    import pandas as pd # <-- Add import inside function
    """使用 Alpha Vantage 获取公司财务指标数据

    通过 FundamentalData 接口获取公司概览、年报数据，并计算各项财务比率和增长率。
    返回的列表中包含一个支持 model_dump() 方法的 Metrics 对象。
    
    同时保存三张财务报表（利润表、资产负债表、现金流量表）的完整数据到数据库和JSON文件中。
    每次调用时，会重新计算财务指标，而不是从数据库中获取。
    """
    # 获取数据库缓存实例
    db_cache = get_db_cache()
    db = get_db()
    
    # 构建缓存参数
    cache_params = {'end': end_date, 'period': period}
    
    try:
        # 检查 API 请求限制
        check_rate_limit()
        
        # 获取公司概览数据
        check_rate_limit()
        try:
            overview, _ = fd.get_company_overview(symbol=ticker)
            if isinstance(overview, list): # <--- 新增检查
                print(f"Warning: fd.get_company_overview returned a list, expected DataFrame. Resetting.")
                overview = pd.DataFrame()
            elif not isinstance(overview, pd.DataFrame):
                print(f"Warning: fd.get_company_overview returned type {type(overview)}, expected DataFrame. Resetting.")
                overview = pd.DataFrame()
        except Exception as e:
            print(f"Error getting company overview: {str(e)}")
            overview = pd.DataFrame() # Ensure it's a DataFrame on error
        print("DEBUG: Fetched overview") # <-- Add Debug Print

        # 获取并保存利润表（年报）数据
        print("DEBUG: Fetching income statement annual...") # <-- Add Debug Print
        check_rate_limit()
        try:
            income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
            if isinstance(income_stmt, list): # <--- 新增检查
                print(f"Warning: fd.get_income_statement_annual returned a list, expected DataFrame. Resetting.")
                income_stmt = pd.DataFrame()
            elif not isinstance(income_stmt, pd.DataFrame):
                print(f"Warning: fd.get_income_statement_annual returned type {type(income_stmt)}, expected DataFrame. Resetting.")
                income_stmt = pd.DataFrame()
        except Exception as e:
            print(f"Error getting income statement: {str(e)}")
            income_stmt = pd.DataFrame() # Ensure it's a DataFrame on error

        # 保存利润表数据到数据库和JSON文件
        if isinstance(income_stmt, pd.DataFrame) and len(income_stmt.index) > 0:
            try:
                # --- Start Detailed Exception Handling ---
                try:
                    income_stmt_dict = income_stmt.to_dict('records')
                except AttributeError as to_dict_ae:
                    print(f"CRITICAL: Failed .to_dict() on income_stmt (annual). Type was: {type(income_stmt)}. Error: {to_dict_ae}")
                    raise # Re-raise the specific error to be caught by outer blocks if needed
                # --- End Detailed Exception Handling ---
                db_cache.set_income_statement_annual(ticker, income_stmt_dict)
                save_to_file_cache('income_statement_annual', ticker, income_stmt_dict, cache_params)
                print(f"已保存 {ticker} 的利润表（年报）数据，共 {len(income_stmt)} 条记录")
            except AttributeError as ae: # Catch re-raised error or other AttributeErrors
                 print(f"AttributeError saving income statement annual: {ae}. Data type was {type(income_stmt)}")
            except Exception as e:
                print(f"Error saving income statement annual: {e}")
        else:
             print(f"Skipping save for income statement annual due to empty or invalid data (type: {type(income_stmt)}).")
        print("DEBUG: Processed income statement annual") # <-- Add Debug Print

        # 获取并保存利润表（季度）数据
        print("DEBUG: Fetching income statement quarterly...") # <-- Add Debug Print
        check_rate_limit()
        try:
            income_stmt_quarterly, _ = fd.get_income_statement_quarterly(symbol=ticker)
            if isinstance(income_stmt_quarterly, list): # <--- 新增检查
                print(f"Warning: fd.get_income_statement_quarterly returned a list, expected DataFrame. Resetting.")
                income_stmt_quarterly = pd.DataFrame()
            elif not isinstance(income_stmt_quarterly, pd.DataFrame):
                print(f"Warning: fd.get_income_statement_quarterly returned type {type(income_stmt_quarterly)}, expected DataFrame. Resetting.")
                income_stmt_quarterly = pd.DataFrame()
        except Exception as e:
            print(f"Error getting quarterly income statement: {str(e)}")
            income_stmt_quarterly = pd.DataFrame() # Ensure it's a DataFrame on error

        # 保存季度利润表数据到数据库和JSON文件
        if isinstance(income_stmt_quarterly, pd.DataFrame) and len(income_stmt_quarterly.index) > 0:
            try:
                # --- Start Detailed Exception Handling ---
                try:
                    income_stmt_quarterly_dict = income_stmt_quarterly.to_dict('records')
                except AttributeError as to_dict_ae:
                    print(f"CRITICAL: Failed .to_dict() on income_stmt_quarterly. Type was: {type(income_stmt_quarterly)}. Error: {to_dict_ae}")
                    raise # Re-raise the specific error
                # --- End Detailed Exception Handling ---
                db_cache.set_income_statement_quarterly(ticker, income_stmt_quarterly_dict)
                save_to_file_cache('income_statement_quarterly', ticker, income_stmt_quarterly_dict, cache_params)
                print(f"已保存 {ticker} 的利润表（季度）数据，共 {len(income_stmt_quarterly)} 条记录")
            except AttributeError as ae:
                 print(f"AttributeError saving income statement quarterly: {ae}. Data type was {type(income_stmt_quarterly)}")
            except Exception as e:
                print(f"Error saving income statement quarterly: {e}")
        else:
             print(f"Skipping save for income statement quarterly due to empty or invalid data (type: {type(income_stmt_quarterly)}).")
        print("DEBUG: Processed income statement quarterly") # <-- Add Debug Print

        # 获取并保存资产负债表（年报）数据
        print("DEBUG: Fetching balance sheet annual...") # <-- Add Debug Print
        check_rate_limit()
        try:
            balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
            if isinstance(balance_sheet, list): # <--- 新增检查
                print(f"Warning: fd.get_balance_sheet_annual returned a list, expected DataFrame. Resetting.")
                balance_sheet = pd.DataFrame()
            elif not isinstance(balance_sheet, pd.DataFrame):
                print(f"Warning: fd.get_balance_sheet_annual returned type {type(balance_sheet)}, expected DataFrame. Resetting.")
                balance_sheet = pd.DataFrame()
        except Exception as e:
            print(f"Error getting balance sheet: {str(e)}")
            balance_sheet = pd.DataFrame() # Ensure it's a DataFrame on error

        # 保存资产负债表数据到数据库和JSON文件
        if isinstance(balance_sheet, pd.DataFrame) and len(balance_sheet.index) > 0:
            try:
                # --- Start Detailed Exception Handling ---
                try:
                    balance_sheet_dict = balance_sheet.to_dict('records')
                except AttributeError as to_dict_ae:
                    print(f"CRITICAL: Failed .to_dict() on balance_sheet (annual). Type was: {type(balance_sheet)}. Error: {to_dict_ae}")
                    raise # Re-raise the specific error
                # --- End Detailed Exception Handling ---
                db_cache.set_balance_sheet_annual(ticker, balance_sheet_dict)
                save_to_file_cache('balance_sheet_annual', ticker, balance_sheet_dict, cache_params)
                print(f"已保存 {ticker} 的资产负债表（年报）数据，共 {len(balance_sheet)} 条记录")
            except AttributeError as ae:
                 print(f"AttributeError saving balance sheet annual: {ae}. Data type was {type(balance_sheet)}")
            except Exception as e:
                print(f"Error saving balance sheet annual: {e}")
        else:
             print(f"Skipping save for balance sheet annual due to empty or invalid data (type: {type(balance_sheet)}).")
        print("DEBUG: Processed balance sheet annual") # <-- Add Debug Print

        # 获取并保存资产负债表（季度）数据
        print("DEBUG: Fetching balance sheet quarterly...") # <-- Add Debug Print
        check_rate_limit()
        try:
            balance_sheet_quarterly, _ = fd.get_balance_sheet_quarterly(symbol=ticker)
            if isinstance(balance_sheet_quarterly, list): # <--- 新增检查
                print(f"Warning: fd.get_balance_sheet_quarterly returned a list, expected DataFrame. Resetting.")
                balance_sheet_quarterly = pd.DataFrame()
            elif not isinstance(balance_sheet_quarterly, pd.DataFrame):
                print(f"Warning: fd.get_balance_sheet_quarterly returned type {type(balance_sheet_quarterly)}, expected DataFrame. Resetting.")
                balance_sheet_quarterly = pd.DataFrame()
        except Exception as e:
            print(f"Error getting quarterly balance sheet: {str(e)}")
            balance_sheet_quarterly = pd.DataFrame() # Ensure it's a DataFrame on error

        # 保存季度资产负债表数据到数据库和JSON文件
        if isinstance(balance_sheet_quarterly, pd.DataFrame) and len(balance_sheet_quarterly.index) > 0:
            try:
                # --- Start Detailed Exception Handling ---
                try:
                    balance_sheet_quarterly_dict = balance_sheet_quarterly.to_dict('records')
                except AttributeError as to_dict_ae:
                    print(f"CRITICAL: Failed .to_dict() on balance_sheet_quarterly. Type was: {type(balance_sheet_quarterly)}. Error: {to_dict_ae}")
                    raise # Re-raise the specific error
                # --- End Detailed Exception Handling ---
                db_cache.set_balance_sheet_quarterly(ticker, balance_sheet_quarterly_dict)
                save_to_file_cache('balance_sheet_quarterly', ticker, balance_sheet_quarterly_dict, cache_params)
                print(f"已保存 {ticker} 的资产负债表（季度）数据，共 {len(balance_sheet_quarterly)} 条记录")
            except AttributeError as ae:
                 print(f"AttributeError saving balance sheet quarterly: {ae}. Data type was {type(balance_sheet_quarterly)}")
            except Exception as e:
                print(f"Error saving balance sheet quarterly: {e}")
        else:
             print(f"Skipping save for balance sheet quarterly due to empty or invalid data (type: {type(balance_sheet_quarterly)}).")
        print("DEBUG: Processed balance sheet quarterly") # <-- Add Debug Print

        # 获取并保存现金流量表（年报）数据
        print("DEBUG: Fetching cash flow annual...") # <-- Add Debug Print
        check_rate_limit()
        try:
            cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
            if isinstance(cash_flow, list): # <--- 新增检查
                print(f"Warning: fd.get_cash_flow_annual returned a list, expected DataFrame. Resetting.")
                cash_flow = pd.DataFrame()
            elif not isinstance(cash_flow, pd.DataFrame):
                print(f"Warning: fd.get_cash_flow_annual returned type {type(cash_flow)}, expected DataFrame. Resetting.")
                cash_flow = pd.DataFrame()
        except Exception as e:
            print(f"Error getting cash flow: {str(e)}")
            cash_flow = pd.DataFrame() # Ensure it's a DataFrame on error

        # 保存现金流量表数据到数据库和JSON文件
        if isinstance(cash_flow, pd.DataFrame) and len(cash_flow.index) > 0:
            try:
                # --- Start Detailed Exception Handling ---
                try:
                    cash_flow_dict = cash_flow.to_dict('records')
                except AttributeError as to_dict_ae:
                    print(f"CRITICAL: Failed .to_dict() on cash_flow (annual). Type was: {type(cash_flow)}. Error: {to_dict_ae}")
                    raise # Re-raise the specific error
                # --- End Detailed Exception Handling ---
                db_cache.set_cash_flow_annual(ticker, cash_flow_dict)
                save_to_file_cache('cash_flow_annual', ticker, cash_flow_dict, cache_params)
                print(f"已保存 {ticker} 的现金流量表（年报）数据，共 {len(cash_flow)} 条记录")
            except AttributeError as ae:
                 print(f"AttributeError saving cash flow annual: {ae}. Data type was {type(cash_flow)}")
            except Exception as e:
                print(f"Error saving cash flow annual: {e}")
        else:
             print(f"Skipping save for cash flow annual due to empty or invalid data (type: {type(cash_flow)}).")
        print("DEBUG: Processed cash flow annual") # <-- Add Debug Print

        # 获取并保存现金流量表（季度）数据
        print("DEBUG: Fetching cash flow quarterly...") # <-- Add Debug Print
        check_rate_limit()
        try:
            cash_flow_quarterly, _ = fd.get_cash_flow_quarterly(symbol=ticker)
            if isinstance(cash_flow_quarterly, list): # <--- 新增检查
                print(f"Warning: fd.get_cash_flow_quarterly for {ticker} returned a list, expected DataFrame. Resetting.")
                cash_flow_quarterly = pd.DataFrame()
            elif not isinstance(cash_flow_quarterly, pd.DataFrame):
                print(f"Warning: fd.get_cash_flow_quarterly for {ticker} returned type {type(cash_flow_quarterly)}, expected DataFrame. Resetting.")
                cash_flow_quarterly = pd.DataFrame()
            # 检查DataFrame是否真的为空，因为API可能返回一个空的DataFrame结构体表示无数据，而不是抛出异常
            if cash_flow_quarterly.empty:
                print(f"Info: fd.get_cash_flow_quarterly for {ticker} returned an empty DataFrame. This might be due to no data or an API issue not causing an exception.")

        except Exception as e:
            # 更明确地指出这是API调用本身的问题
            print(f"CRITICAL_API_ERROR: Error calling fd.get_cash_flow_quarterly for {ticker}: {str(e)}. Will proceed with empty quarterly cash flow data.")
            cash_flow_quarterly = pd.DataFrame() # Ensure it's a DataFrame on error

        # 保存季度现金流量表数据到数据库和JSON文件
        if isinstance(cash_flow_quarterly, pd.DataFrame) and len(cash_flow_quarterly.index) > 0:
            try:
                # --- Start Detailed Exception Handling ---
                try:
                    cash_flow_quarterly_dict = cash_flow_quarterly.to_dict('records')
                except AttributeError as to_dict_ae:
                    print(f"CRITICAL: Failed .to_dict() on cash_flow_quarterly. Type was: {type(cash_flow_quarterly)}. Error: {to_dict_ae}")
                    raise # Re-raise the specific error
                # --- End Detailed Exception Handling ---
                db_cache.set_cash_flow_quarterly(ticker, cash_flow_quarterly_dict)
                save_to_file_cache('cash_flow_quarterly', ticker, cash_flow_quarterly_dict, cache_params)
                print(f"已保存 {ticker} 的现金流量表（季度）数据，共 {len(cash_flow_quarterly)} 条记录")
            except AttributeError as ae:
                 print(f"AttributeError saving cash flow quarterly: {ae}. Data type was {type(cash_flow_quarterly)}")
            except Exception as e:
                print(f"Error saving cash flow quarterly: {e}")
        else:
             print(f"Skipping save for cash flow quarterly due to empty or invalid data (type: {type(cash_flow_quarterly)}).")
        print("DEBUG: Processed cash flow quarterly") # <-- Add Debug Print
        
        # 计算财务指标
        try:
            operating_cash = 0
            capex = 0
            shares = 0
            
            if "operatingCashflow" in cash_flow.columns and len(cash_flow.index) > 0:
                operating_cash = float(cash_flow["operatingCashflow"].iloc[0])
            
            if "capitalExpenditures" in cash_flow.columns and len(cash_flow.index) > 0:
                capex = float(cash_flow["capitalExpenditures"].iloc[0])
            
            if "SharesOutstanding" in overview.columns and len(overview.index) > 0:
                shares = float(overview["SharesOutstanding"].iloc[0])
            
            free_cash_flow = operating_cash - capex
            free_cash_flow_per_share = free_cash_flow / shares if shares != 0 else 0
            
            # 计算股权发行与回购净额
            issuance_or_purchase_of_equity_shares = 0
            if len(cash_flow.index) > 0:
                # 计算股权发行净额
                equity_issuance = 0
                if "proceedsFromIssuanceOfCommonStock" in cash_flow.columns:
                    common_stock_issuance = cash_flow["proceedsFromIssuanceOfCommonStock"].iloc[0]
                    if common_stock_issuance and common_stock_issuance != 'None':
                        equity_issuance += float(common_stock_issuance)
                if "proceedsFromIssuanceOfPreferredStock" in cash_flow.columns:
                    preferred_stock_issuance = cash_flow["proceedsFromIssuanceOfPreferredStock"].iloc[0]
                    if preferred_stock_issuance and preferred_stock_issuance != 'None':
                        equity_issuance += float(preferred_stock_issuance)
                
                # 计算股权回购净额
                equity_repurchase = 0
                if "paymentsForRepurchaseOfCommonStock" in cash_flow.columns:
                    common_stock_repurchase = cash_flow["paymentsForRepurchaseOfCommonStock"].iloc[0]
                    if common_stock_repurchase and common_stock_repurchase != 'None':
                        equity_repurchase += float(common_stock_repurchase)
                if "paymentsForRepurchaseOfEquity" in cash_flow.columns:
                    equity_repurchase_general = cash_flow["paymentsForRepurchaseOfEquity"].iloc[0]
                    if equity_repurchase_general and equity_repurchase_general != 'None':
                        equity_repurchase += float(equity_repurchase_general)
                if "paymentsForRepurchaseOfPreferredStock" in cash_flow.columns:
                    preferred_stock_repurchase = cash_flow["paymentsForRepurchaseOfPreferredStock"].iloc[0]
                    if preferred_stock_repurchase and preferred_stock_repurchase != 'None':
                        equity_repurchase += float(preferred_stock_repurchase)
                
                # 计算净额
                issuance_or_purchase_of_equity_shares = equity_issuance - equity_repurchase
        except Exception as e:
            print(f"Error calculating equity issuance/repurchase: {str(e)}")
            issuance_or_purchase_of_equity_shares = 0
        
        # 获取财报日期
        report_date = None
        if "fiscalDateEnding" in income_stmt.columns and len(income_stmt.index) > 0:
            report_date = income_stmt["fiscalDateEnding"].iloc[0]
        
        # Helper function to extract scalar value from a Series
        def get_scalar(series):
            val = series.iloc[0]
            if isinstance(val, pd.DataFrame) or isinstance(val, pd.Series):
                return val.item()
            return val

        # 计算财务指标
        metrics_data = {
            "return_on_equity": float(overview["ReturnOnEquityTTM"].iloc[0]) if "ReturnOnEquityTTM" in overview.columns and len(overview.index) > 0 else 0,
            "net_margin": float(overview["ProfitMargin"].iloc[0]) if "ProfitMargin" in overview.columns and len(overview.index) > 0 else 0,
            "operating_margin": float(overview["OperatingMarginTTM"].iloc[0]) if "OperatingMarginTTM" in overview.columns and len(overview.index) > 0 else 0,
            "revenue_growth": calculate_growth(income_stmt, "totalRevenue") if "totalRevenue" in income_stmt.columns and len(income_stmt.index) > 0 else 0,
            "earnings_growth": calculate_growth(income_stmt, "netIncome") if "netIncome" in income_stmt.columns and len(income_stmt.index) > 0 else 0,
            "book_value_growth": calculate_growth(balance_sheet, "totalStockholdersEquity") if "totalStockholdersEquity" in balance_sheet.columns and len(balance_sheet.index) > 0 else 0,
            "current_ratio": float(overview["CurrentRatio"].iloc[0]) if "CurrentRatio" in overview.columns and len(overview.index) > 0 else 0,
            "debt_to_equity": float(overview["DebtToEquityRatio"].iloc[0]) if "DebtToEquityRatio" in overview.columns and len(overview.index) > 0 else 0,
            "price_to_earnings_ratio": float(overview["PERatio"].iloc[0]) if "PERatio" in overview.columns and len(overview.index) > 0 else 0,
            "price_to_book_ratio": float(overview["PriceToBookRatio"].iloc[0]) if "PriceToBookRatio" in overview.columns and len(overview.index) > 0 else 0,
            "price_to_sales_ratio": float(overview["PriceToSalesRatioTTM"].iloc[0]) if "PriceToSalesRatioTTM" in overview.columns and len(overview.index) > 0 else 0,
            "earnings_per_share": float(overview["EPS"].iloc[0]) if "EPS" in overview.columns and len(overview.index) > 0 else 0,
            "free_cash_flow_per_share": free_cash_flow_per_share,
            "issuance_or_purchase_of_equity_shares": issuance_or_purchase_of_equity_shares,
            "enterprise_value": float(overview["EnterpriseValue"].iloc[0]) if "EnterpriseValue" in overview.columns and len(overview.index) > 0 else 0,
            "enterprise_value_to_ebitda_ratio": float(overview["EVToEBITDA"].iloc[0]) if "EVToEBITDA" in overview.columns and len(overview.index) > 0 else 0,
            "market_cap": float(overview["MarketCapitalization"].iloc[0]) if "MarketCapitalization" in overview.columns and len(overview.index) > 0 else 0,
            "report_period": report_date or datetime.now().strftime('%Y-%m-%d')
        }
        metrics = MetricsWrapper(metrics_data)
        result = [metrics]
        
        return result
    except Exception as e:
        print(f"Error fetching financial metrics for {ticker}: {str(e)}")
        default_data = {
            "return_on_equity": 0,
            "net_margin": 0,
            "operating_margin": 0,
            "revenue_growth": 0,
            "earnings_growth": 0,
            "book_value_growth": 0,
            "current_ratio": 0,
            "debt_to_equity": 0,
            "price_to_earnings_ratio": 0,
            "price_to_book_ratio": 0,
            "price_to_sales_ratio": 0,
            "earnings_per_share": 0,
            "free_cash_flow_per_share": 0,
            "issuance_or_purchase_of_equity_shares": 0,
            "enterprise_value": 0,
            "enterprise_value_to_ebitda_ratio": 0,
            "market_cap": 0,
            "report_period": datetime.now().strftime('%Y-%m-%d')
        }
        return [MetricsWrapper(default_data)]

def search_line_items(ticker: str, line_items: list, end_date: str = None, period: str = "ttm", limit: int = 2) -> list:
    """仅查找 Alpha Vantage 已有字段，预留计算字段，并进行字段名转换"""
    """使用 Alpha Vantage 获取指定财报项目

    函数从年报数据中抽取所需的项目（如自由现金流、净利润、收入、经营利润率等），
    返回包含属性访问的 FinancialData 对象列表。
    
    直接从API获取数据，不再依赖已删除的line_items表。
    """
    # 获取数据库缓存实例
    db_cache = get_db_cache()
    db = get_db()
    
    # 构建缓存参数
    cache_params = {'end': end_date, 'period': period, 'items': '_'.join(line_items)}
    
    # 如果数据库中没有数据或需要刷新，则从API获取
    try:
        # 检查 API 请求限制
        check_rate_limit()
        income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        
        check_rate_limit()
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        
        check_rate_limit()
        cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
        
        check_rate_limit()
        overview, _ = fd.get_company_overview(symbol=ticker)

        results = []
        for i in range(min(limit, len(income_stmt))):
            data = {}
            
            # 获取财报日期
            if "fiscalDateEnding" in income_stmt.columns.tolist():
                data["report_period"] = income_stmt["fiscalDateEnding"].iloc[i]
            else:
                data["report_period"] = datetime.now().strftime('%Y-%m-%d')
            for item in line_items:
                mapped_item = FIELD_MAPPING.get(item, item)
                if mapped_item in income_stmt.columns.tolist():
                    data[item] = float(income_stmt[mapped_item].iloc[i])
                elif mapped_item in balance_sheet.columns.tolist():
                    data[item] = float(balance_sheet[mapped_item].iloc[i])
                elif mapped_item in cash_flow.columns.tolist():
                    data[item] = float(cash_flow[mapped_item].iloc[i])
                elif mapped_item in overview.columns.tolist():
                    data[item] = float(overview[mapped_item].iloc[0])
                elif item == "free_cash_flow":
                    try:
                        data[item] = float(cash_flow["operatingCashflow"].iloc[i]) - float(cash_flow["capitalExpenditures"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "earnings_per_share":
                    try:
                        net_income = float(income_stmt["netIncome"].iloc[i])
                        shares = 0
                        if "commonStockSharesOutstanding" in balance_sheet.columns.tolist():
                            shares = float(balance_sheet["commonStockSharesOutstanding"].iloc[i])
                        else:
                            check_rate_limit()
                            overview, _ = fd.get_company_overview(symbol=ticker)
                            if "SharesOutstanding" in overview.columns.tolist():
                                shares = float(overview["SharesOutstanding"].iloc[0])
                        data[item] = net_income / shares if shares != 0 else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "book_value_per_share":
                    try:
                        equity = float(balance_sheet["totalShareholderEquity"].iloc[i])
                        shares = 0
                        if "commonStockSharesOutstanding" in balance_sheet.columns.tolist():
                            shares = float(balance_sheet["commonStockSharesOutstanding"].iloc[i])
                        data[item] = equity / shares if shares != 0 else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "current_assets":
                    try:
                        data[item] = float(balance_sheet["totalCurrentAssets"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "current_liabilities":
                    try:
                        data[item] = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "depreciation_and_amortization":
                    try:
                        data[item] = float(cash_flow["depreciationDepletionAndAmortization"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "capital_expenditure":
                    try:
                        data[item] = float(cash_flow["capitalExpenditures"].iloc[i])
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "working_capital":
                    try:
                        current_assets = float(balance_sheet["totalCurrentAssets"].iloc[i])
                        current_liabilities = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                        data[item] = current_assets - current_liabilities
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "revenue":
                    try:
                        data[item] = float(income_stmt["totalRevenue"].iloc[i]) if "totalRevenue" in income_stmt.columns.tolist() else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "operating_margin":
                    if "OperatingMarginTTM" in overview.columns.tolist():
                        data[item] = float(overview["OperatingMarginTTM"].iloc[0])
                elif item == "debt_to_equity":
                    try:
                        total_liabilities = float(balance_sheet["totalLiabilities"].iloc[i])
                        shareholders_equity = float(balance_sheet["totalShareholderEquity"].iloc[i])
                        data[item] = total_liabilities / shareholders_equity if shareholders_equity != 0 else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "total_assets":
                    try:
                        data[item] = float(balance_sheet["totalAssets"].iloc[i]) if "totalAssets" in balance_sheet.columns.tolist() else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "total_liabilities":
                    try:
                        data[item] = float(balance_sheet["totalLiabilities"].iloc[i]) if "totalLiabilities" in balance_sheet.columns.tolist() else 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "dividends_and_other_cash_distributions":
                    data[item] = 0  # ALPHA VANTAGE 暂未提供此项数据
                # 添加缺失的项目处理
                elif item == "outstanding_shares":
                    try:
                        if "commonStockSharesOutstanding" in balance_sheet.columns.tolist():
                            data[item] = float(balance_sheet["commonStockSharesOutstanding"].iloc[i])
                        else:
                            if "SharesOutstanding" in overview.columns.tolist():
                                data[item] = float(overview["SharesOutstanding"].iloc[0])
                            else:
                                data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "operating_income":
                    try:
                        if "operatingIncome" in income_stmt.columns.tolist():
                            data[item] = float(income_stmt["operatingIncome"].iloc[i])
                        elif "totalRevenue" in income_stmt.columns.tolist() and "operatingExpenses" in income_stmt.columns.tolist():
                            revenue = float(income_stmt["totalRevenue"].iloc[i])
                            operating_expenses = float(income_stmt["operatingExpenses"].iloc[i])
                            data[item] = revenue - operating_expenses
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "return_on_invested_capital":
                    try:
                        if "netIncome" in income_stmt.columns.tolist() and "totalAssets" in balance_sheet.columns.tolist() and "totalCurrentLiabilities" in balance_sheet.columns.tolist():
                            net_income = float(income_stmt["netIncome"].iloc[i])
                            total_assets = float(balance_sheet["totalAssets"].iloc[i])
                            current_liabilities = float(balance_sheet["totalCurrentLiabilities"].iloc[i])
                            invested_capital = total_assets - current_liabilities
                            data[item] = net_income / invested_capital if invested_capital != 0 else 0
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "cash_and_equivalents":
                    try:
                        if "cashAndCashEquivalentsAtCarryingValue" in balance_sheet.columns.tolist():
                            data[item] = float(balance_sheet["cashAndCashEquivalentsAtCarryingValue"].iloc[i])
                        elif "cashAndShortTermInvestments" in balance_sheet.columns.tolist():
                            data[item] = float(balance_sheet["cashAndShortTermInvestments"].iloc[i])
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "total_debt":
                    try:
                        if "shortLongTermDebtTotal" in balance_sheet.columns.tolist():
                            data[item] = float(balance_sheet["shortLongTermDebtTotal"].iloc[i])
                        else:
                            short_term_debt = float(balance_sheet["shortTermDebt"].iloc[i]) if "shortTermDebt" in balance_sheet.columns.tolist() else 0
                            long_term_debt = float(balance_sheet["longTermDebt"].iloc[i]) if "longTermDebt" in balance_sheet.columns.tolist() else 0
                            current_long_term_debt = float(balance_sheet["currentLongTermDebt"].iloc[i]) if "currentLongTermDebt" in balance_sheet.columns.tolist() else 0
                            data[item] = short_term_debt + long_term_debt + current_long_term_debt
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "shareholders_equity":
                    try:
                        if "totalShareholderEquity" in balance_sheet.columns.tolist():
                            data[item] = float(balance_sheet["totalShareholderEquity"].iloc[i])
                        elif "totalStockholdersEquity" in balance_sheet.columns.tolist():
                            data[item] = float(balance_sheet["totalStockholdersEquity"].iloc[i])
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "operating_expense":
                    try:
                        if "operatingExpenses" in income_stmt.columns.tolist():
                            data[item] = float(income_stmt["operatingExpenses"].iloc[i])
                        elif "totalOperatingExpenses" in income_stmt.columns.tolist():
                            data[item] = float(income_stmt["totalOperatingExpenses"].iloc[i])
                        elif "totalRevenue" in income_stmt.columns.tolist() and "operatingIncome" in income_stmt.columns.tolist():
                            revenue = float(income_stmt["totalRevenue"].iloc[i])
                            op_income = float(income_stmt["operatingIncome"].iloc[i])
                            data[item] = revenue - op_income
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "ebit":
                    try:
                        # 使用营业利润率和总收入计算 EBIT
                        operating_margin = float(overview["OperatingMarginTTM"].iloc[0])
                        revenue = float(overview["RevenueTTM"].iloc[0])
                        data[item] = operating_margin * revenue
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "ebitda":
                    try:
                        # 直接从 overview 获取
                        if "EBITDA" in overview.columns.tolist():
                            data[item] = float(overview["EBITDA"].iloc[0])
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "goodwill_and_intangible_assets":
                    try:
                        goodwill = 0
                        intangible_assets = 0
                        if "goodwill" in balance_sheet.columns.tolist():
                            goodwill_val = balance_sheet["goodwill"].iloc[i]
                            if goodwill_val and goodwill_val != 'None':
                                goodwill = float(goodwill_val)
                        if "intangibleAssets" in balance_sheet.columns.tolist():
                            intangible_val = balance_sheet["intangibleAssets"].iloc[i]
                            if intangible_val and intangible_val != 'None':
                                intangible_assets = float(intangible_val)
                        data[item] = goodwill + intangible_assets
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "gross_margin":
                    try:
                        total_revenue = float(income_stmt["totalRevenue"].iloc[i]) if "totalRevenue" in income_stmt.columns.tolist() else 0
                        if total_revenue:
                            if "grossProfit" in income_stmt.columns.tolist():
                                gross_profit = float(income_stmt["grossProfit"].iloc[i])
                                data[item] = gross_profit / total_revenue
                            elif "costOfRevenue" in income_stmt.columns.tolist():
                                cost_rev = float(income_stmt["costOfRevenue"].iloc[i])
                                data[item] = (total_revenue - cost_rev) / total_revenue
                            else:
                                data[item] = 0
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "research_and_development":
                    try:
                        if "researchAndDevelopment" in income_stmt.columns.tolist():
                            data[item] = float(income_stmt["researchAndDevelopment"].iloc[i])
                        elif "researchAndDevelopmentExpense" in income_stmt.columns.tolist():
                            data[item] = float(income_stmt["researchAndDevelopmentExpense"].iloc[i])
                        else:
                            data[item] = 0
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                elif item == "issuance_or_purchase_of_equity_shares":
                    try:
                        # 计算股权发行净额
                        equity_issuance = 0
                        if "proceedsFromIssuanceOfCommonStock" in cash_flow.columns.tolist():
                            common_stock_issuance = cash_flow["proceedsFromIssuanceOfCommonStock"].iloc[i]
                            if common_stock_issuance and common_stock_issuance != 'None':
                                equity_issuance += float(common_stock_issuance)
                        if "proceedsFromIssuanceOfPreferredStock" in cash_flow.columns.tolist():
                            preferred_stock_issuance = cash_flow["proceedsFromIssuanceOfPreferredStock"].iloc[i]
                            if preferred_stock_issuance and preferred_stock_issuance != 'None':
                                equity_issuance += float(preferred_stock_issuance)
                        
                        # 计算股权回购净额
                        equity_repurchase = 0
                        if "paymentsForRepurchaseOfCommonStock" in cash_flow.columns.tolist():
                            common_stock_repurchase = cash_flow["paymentsForRepurchaseOfCommonStock"].iloc[i]
                            if common_stock_repurchase and common_stock_repurchase != 'None':
                                equity_repurchase += float(common_stock_repurchase)
                        if "paymentsForRepurchaseOfEquity" in cash_flow.columns.tolist():
                            equity_repurchase_general = cash_flow["paymentsForRepurchaseOfEquity"].iloc[i]
                            if equity_repurchase_general and equity_repurchase_general != 'None':
                                equity_repurchase += float(equity_repurchase_general)
                        if "paymentsForRepurchaseOfPreferredStock" in cash_flow.columns.tolist():
                            preferred_stock_repurchase = cash_flow["paymentsForRepurchaseOfPreferredStock"].iloc[i]
                            if preferred_stock_repurchase and preferred_stock_repurchase != 'None':
                                equity_repurchase += float(preferred_stock_repurchase)
                        
                        # 计算净额
                        data[item] = equity_issuance - equity_repurchase
                    except Exception as e:
                        print(f"Error processing {item}: {str(e)}")
                        data[item] = 0
                else:
                    print(f"Warning: Unknown line item {item}")
            results.append(MetricsWrapper(data))
        return results
    except Exception as e:
        print(f"Error fetching line items for {ticker}: {str(e)}")
    return []

def check_and_update_financials(ticker: str):
    """
    检查缓存是否需要更新，必要时下载并保存最新财务数据
    """
    try:
        if should_refresh_financial_data(ticker):
            print(f"检测到 {ticker} 财报需要更新，开始下载...")
            get_financial_metrics(ticker)
            print(f"{ticker} 财报已更新完成。")
        else:
            print(f"{ticker} 财报缓存有效，无需更新。")
    except Exception as e:
        print(f"检查更新财报失败: {e}")

def get_income_statement(ticker: str, period: str = "annual"):
    """获取利润表数据
    
    Args:
        ticker: 股票代码
        period: 周期，可选 "annual" 或 "quarterly"，默认为 "annual"
    
    Returns:
        利润表数据列表
    """
    check_rate_limit()
    
    try:
        if period.lower() == "annual":
            income_stmt, _ = fd.get_income_statement_annual(symbol=ticker)
        else:
            income_stmt, _ = fd.get_income_statement_quarterly(symbol=ticker)
        
        if len(income_stmt.index) == 0:
            print(f"没有找到 {ticker} 的利润表数据")
            return []
        
        # 转换为字典列表
        return income_stmt.to_dict('records')
    except Exception as e:
        print(f"获取 {ticker} 的利润表数据时出错: {str(e)}")
        return []

def get_balance_sheet(ticker: str, period: str = "annual"):
    """获取资产负债表数据
    
    Args:
        ticker: 股票代码
        period: 周期，可选 "annual" 或 "quarterly"，默认为 "annual"
    
    Returns:
        资产负债表数据列表
    """
    check_rate_limit()
    
    try:
        if period.lower() == "annual":
            balance_sheet, _ = fd.get_balance_sheet_annual(symbol=ticker)
        else:
            balance_sheet, _ = fd.get_balance_sheet_quarterly(symbol=ticker)
        
        if len(balance_sheet.index) == 0:
            print(f"没有找到 {ticker} 的资产负债表数据")
            return []
        
        # 转换为字典列表
        return balance_sheet.to_dict('records')
    except Exception as e:
        print(f"获取 {ticker} 的资产负债表数据时出错: {str(e)}")
        return []

def get_cash_flow(ticker: str, period: str = "annual"):
    """获取现金流量表数据
    
    Args:
        ticker: 股票代码
        period: 周期，可选 "annual" 或 "quarterly"，默认为 "annual"
    
    Returns:
        现金流量表数据列表
    """
    check_rate_limit()
    
    try:
        if period.lower() == "annual":
            cash_flow, _ = fd.get_cash_flow_annual(symbol=ticker)
        else:
            cash_flow, _ = fd.get_cash_flow_quarterly(symbol=ticker)
        
        if len(cash_flow.index) == 0:
            print(f"没有找到 {ticker} 的现金流量表数据")
            return []
        
        # 转换为字典列表
        return cash_flow.to_dict('records')
    except Exception as e:
        print(f"获取 {ticker} 的现金流量表数据时出错: {str(e)}")
        return []
