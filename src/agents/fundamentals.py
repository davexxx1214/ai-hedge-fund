from langchain_core.messages import HumanMessage

from graph.state import AgentState, show_agent_reasoning

import json

from tools.api import get_financial_metrics


##### Fundamental Agent #####
def fundamentals_agent(state: AgentState):
    """Fundamental analysis agent that evaluates company financials."""
    data = state["data"]
    
    # 获取财务指标
    financial_data = get_financial_metrics(
        ticker=data["ticker"],
        report_period=data["end_date"],
        period="ttm",
        limit=1,
    )
    
    # 从财务数据字典中获取最新的数据
    # yfinance 返回的是字典格式，我们需要获取最新的数据
    metrics = {
        "revenue_growth": 0,  # 默认值
        "profit_margin": 0,
        "earnings_growth": 0,
        "debt_to_equity": 0,
        "current_ratio": 0,
    }
    
    try:
        # 尝试从财务数据中提取相关指标
        if 'Total Revenue' in financial_data:
            revenue = financial_data['Total Revenue']
            metrics["revenue_growth"] = ((revenue.iloc[0] - revenue.iloc[1]) / revenue.iloc[1]) if len(revenue) > 1 else 0
            
        if 'Net Income' in financial_data and 'Total Revenue' in financial_data:
            net_income = financial_data['Net Income'].iloc[0]
            total_revenue = financial_data['Total Revenue'].iloc[0]
            metrics["profit_margin"] = (net_income / total_revenue) if total_revenue != 0 else 0
            
        if 'Net Income' in financial_data:
            net_income = financial_data['Net Income']
            metrics["earnings_growth"] = ((net_income.iloc[0] - net_income.iloc[1]) / abs(net_income.iloc[1])) if len(net_income) > 1 and net_income.iloc[1] != 0 else 0
            
    except (KeyError, IndexError) as e:
        print(f"Warning: Some financial metrics could not be calculated: {e}")
    
    # 计算基本面信号
    signal = "neutral"
    confidence = 50
    
    # 基于财务指标评估信号
    positive_signals = 0
    total_signals = 0
    
    if metrics["revenue_growth"] > 0.1:  # 10% 增长
        positive_signals += 1
    total_signals += 1
    
    if metrics["profit_margin"] > 0.15:  # 15% 利润率
        positive_signals += 1
    total_signals += 1
    
    if metrics["earnings_growth"] > 0.1:  # 10% 增长
        positive_signals += 1
    total_signals += 1
    
    # 计算信号和置信度
    if total_signals > 0:
        score = positive_signals / total_signals
        if score > 0.7:
            signal = "bullish"
            confidence = int(score * 100)
        elif score < 0.3:
            signal = "bearish"
            confidence = int((1 - score) * 100)
        else:
            signal = "neutral"
            confidence = 50
    
    # 创建推理说明
    reasoning = {
        "fundamentals": {
            "signal": signal,
            "metrics": metrics,
            "details": f"Revenue Growth: {metrics['revenue_growth']:.1%}, "
                      f"Profit Margin: {metrics['profit_margin']:.1%}, "
                      f"Earnings Growth: {metrics['earnings_growth']:.1%}"
        }
    }
    
    message_content = {
        "signal": signal,
        "confidence": confidence,
        "reasoning": reasoning,
    }
    
    # 创建消息
    message = HumanMessage(
        content=json.dumps(message_content),
        name="fundamentals_agent",
    )
    
    # 显示推理过程（如果需要）
    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(message_content, "Fundamental Analysis Agent")
    
    # 添加信号到分析结果中
    state["data"]["analyst_signals"]["fundamentals_agent"] = {
        "signal": signal,
        "confidence": confidence,
        "reasoning": reasoning,
    }
    
    return {
        "messages": [message],
        "data": data,
    }