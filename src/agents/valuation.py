from langchain_core.messages import HumanMessage
from graph.state import AgentState, show_agent_reasoning
import json

from tools.api import get_financial_metrics, get_market_cap, search_line_items

def valuation_agent(state: AgentState):
    """进行估值分析并生成交易信号"""
    data = state["data"]
    
    # 获取财务指标
    metrics = get_financial_metrics(
        ticker=data["ticker"],
        period="quarterly",
        end_date=data["end_date"]
    )
    
    # 计算不同估值方法的结果
    market_cap = metrics["MarketCap"]
    net_income = metrics["NetIncome"]
    pe_ratio = metrics["PE"]
    
    # 初始化分析结果
    signal = "neutral"
    confidence = 50
    reasons = {}
    
    # 基于PE的估值分析
    if pe_ratio > 0:
        industry_avg_pe = 20  # 假设行业平均PE为20
        if pe_ratio < industry_avg_pe * 0.7:
            reasons["pe_valuation"] = {"signal": "bullish", "detail": f"PE ({pe_ratio:.2f}) 显著低于行业平均"}
        elif pe_ratio > industry_avg_pe * 1.3:
            reasons["pe_valuation"] = {"signal": "bearish", "detail": f"PE ({pe_ratio:.2f}) 显著高于行业平均"}
        else:
            reasons["pe_valuation"] = {"signal": "neutral", "detail": f"PE ({pe_ratio:.2f}) 接近行业平均"}
    
    # 基于市值和净收入的分析
    if net_income > 0:
        price_to_earnings = market_cap / net_income
        if price_to_earnings < 15:
            reasons["price_earnings"] = {"signal": "bullish", "detail": f"市值收益比 ({price_to_earnings:.2f}) 较低"}
        elif price_to_earnings > 25:
            reasons["price_earnings"] = {"signal": "bearish", "detail": f"市值收益比 ({price_to_earnings:.2f}) 较高"}
        else:
            reasons["price_earnings"] = {"signal": "neutral", "detail": f"市值收益比 ({price_to_earnings:.2f}) 适中"}
    
    # 统计各种信号的数量
    signal_counts = {'bullish': 0, 'bearish': 0, 'neutral': 0}
    for analysis in reasons.values():
        signal_counts[analysis['signal']] += 1
    
    # 确定最终信号
    max_count = max(signal_counts.values())
    if max_count == signal_counts['bullish'] and signal_counts['bullish'] > signal_counts['bearish']:
        signal = 'bullish'
    elif max_count == signal_counts['bearish'] and signal_counts['bearish'] > signal_counts['bullish']:
        signal = 'bearish'
    
    # 计算置信度
    total_signals = len(reasons)
    if total_signals > 0:
        confidence = (max_count / total_signals) * 100
    
    # 构建消息内容
    message_content = {
        "signal": signal,
        "confidence": round(confidence, 2),
        "reasoning": reasons
    }
    
    message = HumanMessage(
        content=json.dumps(message_content),
        name="valuation_agent"
    )
    
    # 显示推理过程（如果需要）
    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(message_content, "Valuation Analysis Agent")
    
    # 将分析结果添加到状态中
    state["data"]["analyst_signals"]["valuation_agent"] = message_content
    
    return {
        "messages": [message],
        "data": data
    }

def calculate_owner_earnings_value(
    net_income: float,
    depreciation: float,
    capex: float,
    working_capital_change: float,
    growth_rate: float = 0.05,
    required_return: float = 0.15,
    margin_of_safety: float = 0.25,
    num_years: int = 5,
) -> float:
    """
    Calculates the intrinsic value using Buffett's Owner Earnings method.

    Owner Earnings = Net Income
                    + Depreciation/Amortization
                    - Capital Expenditures
                    - Working Capital Changes

    Args:
        net_income: Annual net income
        depreciation: Annual depreciation and amortization
        capex: Annual capital expenditures
        working_capital_change: Annual change in working capital
        growth_rate: Expected growth rate
        required_return: Required rate of return (Buffett typically uses 15%)
        margin_of_safety: Margin of safety to apply to final value
        num_years: Number of years to project

    Returns:
        float: Intrinsic value with margin of safety
    """
    if not all(
        [
            isinstance(x, (int, float))
            for x in [net_income, depreciation, capex, working_capital_change]
        ]
    ):
        return 0

    # Calculate initial owner earnings
    owner_earnings = net_income + depreciation - capex - working_capital_change

    if owner_earnings <= 0:
        return 0

    # Project future owner earnings
    future_values = []
    for year in range(1, num_years + 1):
        future_value = owner_earnings * (1 + growth_rate) ** year
        discounted_value = future_value / (1 + required_return) ** year
        future_values.append(discounted_value)

    # Calculate terminal value (using perpetuity growth formula)
    terminal_growth = min(growth_rate, 0.03)  # Cap terminal growth at 3%
    terminal_value = (future_values[-1] * (1 + terminal_growth)) / (
        required_return - terminal_growth
    )
    terminal_value_discounted = terminal_value / (1 + required_return) ** num_years

    # Sum all values and apply margin of safety
    intrinsic_value = sum(future_values) + terminal_value_discounted
    value_with_safety_margin = intrinsic_value * (1 - margin_of_safety)

    return value_with_safety_margin


def calculate_intrinsic_value(
    free_cash_flow: float,
    growth_rate: float = 0.05,
    discount_rate: float = 0.10,
    terminal_growth_rate: float = 0.02,
    num_years: int = 5,
) -> float:
    """
    Computes the discounted cash flow (DCF) for a given company based on the current free cash flow.
    Use this function to calculate the intrinsic value of a stock.
    """
    # Estimate the future cash flows based on the growth rate
    cash_flows = [free_cash_flow * (1 + growth_rate) ** i for i in range(num_years)]

    # Calculate the present value of projected cash flows
    present_values = []
    for i in range(num_years):
        present_value = cash_flows[i] / (1 + discount_rate) ** (i + 1)
        present_values.append(present_value)

    # Calculate the terminal value
    terminal_value = (
        cash_flows[-1]
        * (1 + terminal_growth_rate)
        / (discount_rate - terminal_growth_rate)
    )
    terminal_present_value = terminal_value / (1 + discount_rate) ** num_years

    # Sum up the present values and terminal value
    dcf_value = sum(present_values) + terminal_present_value

    return dcf_value


def calculate_working_capital_change(
    current_working_capital: float,
    previous_working_capital: float,
) -> float:
    """
    Calculate the absolute change in working capital between two periods.
    A positive change means more capital is tied up in working capital (cash outflow).
    A negative change means less capital is tied up (cash inflow).

    Args:
        current_working_capital: Current period's working capital
        previous_working_capital: Previous period's working capital

    Returns:
        float: Change in working capital (current - previous)
    """
    return current_working_capital - previous_working_capital
