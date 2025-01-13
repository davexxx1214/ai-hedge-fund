from langchain_core.messages import HumanMessage
from graph.state import AgentState, show_agent_reasoning
import pandas as pd
import numpy as np
import json

from tools.api import get_insider_trades

##### Sentiment Agent #####
def sentiment_agent(state: AgentState):
    """分析市场情绪并生成交易信号"""
    data = state["data"]

    # 获取内部交易数据
    insider_trades = get_insider_trades(
        ticker=data["ticker"],
        limit=10,
        end_date=data["end_date"]
    )
    
    # 统计信号
    bullish_signals = 0
    bearish_signals = 0
    
    # 分析内部交易
    for trade in insider_trades:
        if trade['transaction_type'].upper() in ['BUY', 'PURCHASE']:
            bullish_signals += 1
        elif trade['transaction_type'].upper() in ['SELL', 'SALE']:
            bearish_signals += 1
    
    total_signals = bullish_signals + bearish_signals
    
    # 处理没有交易信号的情况
    if total_signals == 0:
        signal = "neutral"
        confidence = 50  # 默认置信度
    else:
        # 确定信号方向
        if bullish_signals > bearish_signals:
            signal = "bullish"
        elif bearish_signals > bullish_signals:
            signal = "bearish"
        else:
            signal = "neutral"
        
        # 计算置信度
        confidence = round(max(bullish_signals, bearish_signals) / total_signals, 2) * 100
    
    # 构建推理过程
    reasoning = {
        "insider_activity": {
            "bullish_trades": bullish_signals,
            "bearish_trades": bearish_signals,
            "total_trades": total_signals
        }
    }
    
    message_content = {
        "signal": signal,
        "confidence": confidence,
        "reasoning": reasoning
    }
    
    message = HumanMessage(
        content=json.dumps(message_content),
        name="sentiment_agent"
    )
    
    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(message_content, "Market Sentiment Agent")
    
    # 添加信号到分析结果
    state["data"]["analyst_signals"]["sentiment_agent"] = {
        "signal": signal,
        "confidence": confidence,
        "reasoning": reasoning
    }
    
    return {
        "messages": [message],
        "data": data
    }