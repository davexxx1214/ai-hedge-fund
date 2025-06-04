#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""测试缺失的财务指标"""

from src.tools.api_financials import search_line_items

def test_missing_items():
    """测试之前缺失的财务指标"""
    print("=== 测试缺失的财务指标 ===")
    
    # 测试之前缺失的指标
    missing_items = [
        'debt_to_equity',
        'dividends_and_other_cash_distributions', 
        'book_value_per_share',
        'operating_expense'
    ]
    
    result = search_line_items('AAPL', missing_items, limit=2)
    
    for i, record in enumerate(result):
        print(f"记录 {i+1}:")
        print(f"  report_period: {record.report_period}")
        for item in missing_items:
            value = getattr(record, item, 'N/A')
            print(f"  {item}: {value}")
        print()

if __name__ == "__main__":
    test_missing_items() 