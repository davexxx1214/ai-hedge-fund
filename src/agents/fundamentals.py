from langchain_core.messages import HumanMessage

from graph.state import AgentState, show_agent_reasoning

import json

from tools.api import get_financial_metrics

def fundamentals_agent(state: AgentState):
    """分析基本面数据并生成交易信号"""
    data = state["data"]
    
    metrics = get_financial_metrics(
        ticker=data["ticker"],
        period="quarterly",
        end_date=data["end_date"]
    )
    
    signals = []  # 初始化signals列表
    reasoning = {}  # 初始化reasoning字典
    
    # 1. Profitability Analysis
    return_on_equity = metrics.get("ROE", 0)
    net_margin = metrics.get("ProfitMargin", 0)
    operating_margin = metrics.get("OperatingMarginTTM", 0)

    profitability_score = 0
    thresholds = [
        (return_on_equity, 0.15),  # Strong ROE above 15%
        (net_margin, 0.20),  # Healthy profit margins
        (operating_margin, 0.15),  # Strong operating efficiency
    ]
    
    for metric, threshold in thresholds:
        if metric > threshold:
            profitability_score += 1

    signals.append(
        "bullish" if profitability_score >= 2
        else "bearish" if profitability_score == 0 
        else "neutral"
    )
    
    reasoning["profitability_signal"] = {
        "signal": signals[0],
        "details": (
            f"ROE: {return_on_equity:.2%}, "
            f"Net Margin: {net_margin:.2%}, "
            f"Op Margin: {operating_margin:.2%}"
        )
    }

    # 2. Valuation Analysis
    pe_ratio = metrics.get("PE", 0)
    pb_ratio = metrics.get("PB", 0)

    valuation_score = 0
    if 0 < pe_ratio < 15: valuation_score += 1
    if 0 < pb_ratio < 3: valuation_score += 1

    signals.append(
        "bullish" if valuation_score == 2
        else "bearish" if valuation_score == 0
        else "neutral"
    )
    
    reasoning["valuation_signal"] = {
        "signal": signals[1],
        "details": f"P/E: {pe_ratio:.2f}, P/B: {pb_ratio:.2f}"
    }

    # Determine overall signal
    bullish_signals = signals.count("bullish")
    bearish_signals = signals.count("bearish")
    
    if bullish_signals > bearish_signals:
        overall_signal = "bullish"
    elif bearish_signals > bullish_signals:
        overall_signal = "bearish"
    else:
        overall_signal = "neutral"

    # Calculate confidence level
    total_signals = len(signals)
    confidence = round(max(bullish_signals, bearish_signals) / total_signals * 100)

    message_content = {
        "signal": overall_signal,
        "confidence": confidence,
        "reasoning": reasoning
    }

    message = HumanMessage(
        content=json.dumps(message_content),
        name="fundamentals_agent"
    )

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(message_content, "Fundamental Analysis Agent")

    state["data"]["analyst_signals"]["fundamentals_agent"] = message_content

    return {
        "messages": [message],
        "data": data
    }