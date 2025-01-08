from langchain_core.messages import HumanMessage
from graph.state import AgentState, show_agent_reasoning
import json
import yfinance as yf
from tools.api import get_financial_metrics, get_market_cap


def valuation_agent(state: AgentState):
    """Performs detailed valuation analysis using multiple methodologies."""
    data = state["data"]
    
    try:
        # 获取财务数据
        financial_metrics = get_financial_metrics(
            ticker=data["ticker"],
            report_period=data["end_date"],
        )
        
        # 设置默认的财务指标
        metrics = {
            "revenue_growth": 0,
            "profit_margin": 0,
            "net_income": 0,
            "total_assets": 0,
            "total_liabilities": 0,
            "working_capital_change": 0,
        }
        
        # 安全地获取财务数据
        if financial_metrics is not None:
            if 'Total Revenue' in financial_metrics and len(financial_metrics['Total Revenue']) >= 2:
                revenue = financial_metrics['Total Revenue']
                metrics["revenue_growth"] = ((revenue.iloc[0] - revenue.iloc[1]) / revenue.iloc[1]) if revenue.iloc[1] != 0 else 0
            
            if 'Net Income' in financial_metrics and len(financial_metrics['Net Income']) > 0:
                metrics["net_income"] = financial_metrics['Net Income'].iloc[0]
            
            if 'Total Assets' in financial_metrics and len(financial_metrics['Total Assets']) > 0:
                metrics["total_assets"] = financial_metrics['Total Assets'].iloc[0]
            
            if 'Total Liabilities' in financial_metrics and len(financial_metrics['Total Liabilities']) > 0:
                metrics["total_liabilities"] = financial_metrics['Total Liabilities'].iloc[0]
        
        # 计算估值信号
        signal = "neutral"
        confidence = 50
        
        # 基于财务指标评估信号
        score = 0
        valid_metrics = 0
        
        if metrics["revenue_growth"] > 0.1:  # 10% 收入增长
            score += 1
            valid_metrics += 1
        
        if metrics["profit_margin"] > 0.15:  # 15% 利润率
            score += 1
            valid_metrics += 1
        
        if metrics["total_assets"] > metrics["total_liabilities"]:  # 健康的资产负债率
            score += 1
            valid_metrics += 1
        
        if valid_metrics > 0:
            final_score = score / valid_metrics
            if final_score > 0.7:
                signal = "bullish"
                confidence = int(final_score * 100)
            elif final_score < 0.3:
                signal = "bearish"
                confidence = int((1 - final_score) * 100)
        
        # 创建估值结果
        valuation_result = {
            "signal": signal,
            "confidence": confidence,
            "metrics": metrics
        }
        
        message = HumanMessage(
            content=json.dumps(valuation_result),
            name="valuation_agent",
        )
        
        if state["metadata"]["show_reasoning"]:
            show_agent_reasoning(valuation_result, "Valuation Analysis")
        
        state["data"]["analyst_signals"]["valuation_agent"] = {
            "signal": signal,
            "confidence": confidence,
            "reasoning": metrics,
        }
        
        return {
            "messages": [message],
            "data": data,
        }
        
    except Exception as e:
        print(f"Warning in valuation agent: {e}")
        # 返回中性信号
        default_result = {
            "signal": "neutral",
            "confidence": 50,
            "reasoning": "Error processing financial data"
        }
        
        message = HumanMessage(
            content=json.dumps(default_result),
            name="valuation_agent",
        )
        
        state["data"]["analyst_signals"]["valuation_agent"] = default_result
        
        return {
            "messages": [message],
            "data": data,
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
