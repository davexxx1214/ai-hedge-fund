from langchain_core.messages import HumanMessage
from graph.state import AgentState, show_agent_reasoning
import pandas as pd
import numpy as np
import json

from tools.api import get_insider_trades

##### Sentiment Agent #####


def sentiment_agent(state: AgentState):
    """Market sentiment analysis agent."""
    data = state["data"]
    
    # 移除内部交易相关的代码
    # insider_trades = get_insider_trades(...)
    
    # 创建情绪分析结果
    signal = "neutral"  # 默认信号
    confidence = 50  # 默认置信度
    
    reasoning = {
        "market_sentiment": {
            "signal": signal,
            "details": "Market sentiment analysis is currently limited as insider trading data is not available"
        }
    }
    
    message_content = {
        "signal": signal,
        "confidence": confidence,
        "reasoning": reasoning,
    }
    
    message = HumanMessage(
        content=json.dumps(message_content),
        name="sentiment_agent",
    )
    
    # Print the reasoning if the flag is set
    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(message_content, "Sentiment Analysis Agent")
    
    # Add the signal to the analyst_signals list
    state["data"]["analyst_signals"]["sentiment_agent"] = {
        "signal": signal,
        "confidence": confidence,
        "reasoning": reasoning,
    }
    
    return {
        "messages": [message],
        "data": data,
    }