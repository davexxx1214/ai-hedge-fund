#!/usr/bin/env python3
"""
测试财务API的新缓存逻辑
"""

from src.tools.api_financials import get_financial_metrics, search_line_items
import json

def test_financial_metrics():
    print("=== 测试 get_financial_metrics ===")
    result = get_financial_metrics('AAPL')
    if result:
        print("获取到财务指标:")
        metrics = result[0].model_dump()
        for key, value in metrics.items():
            print(f"  {key}: {value}")
        print()
    else:
        print("未获取到财务指标")

def test_search_line_items():
    print("=== 测试 search_line_items ===")
    line_items = ['revenue', 'net_income', 'free_cash_flow', 'working_capital', 'total_assets']
    result = search_line_items('AAPL', line_items, limit=2)
    
    if result:
        print(f"获取到 {len(result)} 条line items记录:")
        for i, item in enumerate(result):
            print(f"记录 {i+1}:")
            data = item.model_dump()
            for key, value in data.items():
                print(f"  {key}: {value}")
            print()
    else:
        print("未获取到line items")

if __name__ == "__main__":
    test_financial_metrics()
    test_search_line_items() 