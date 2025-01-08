import math
from langchain_core.messages import HumanMessage
from graph.state import AgentState, show_agent_reasoning
from tools.api import get_prices
import pandas as pd
import json


##### Risk Management Agent #####
def risk_management_agent(state: AgentState):
    """风险管理代理"""
    data = state["data"]
    
    try:
        # 获取价格数据
        prices_df = get_prices(
            ticker=data["ticker"],
            start_date=data["start_date"],
            end_date=data["end_date"],
        )
        
        # 处理多级索引列
        if isinstance(prices_df.columns, pd.MultiIndex):
            prices_df.columns = prices_df.columns.get_level_values(0)
        
        # 将列名转换为小写
        prices_df.columns = prices_df.columns.str.lower()
        
        # 计算风险指标
        current_price = prices_df["close"].iloc[-1]
        price_change = prices_df["close"].pct_change().iloc[-1]
        volatility = prices_df["close"].pct_change().std() * math.sqrt(252)
        
        # 根据波动率调整最大仓位
        base_position_size = 0.2  # 基础仓位 20%
        max_position_size = base_position_size * (1 - min(volatility, 0.5))  # 根据波动率调整
        
        # 评估风险水平
        risk_score = 0
        if volatility > 0.3:  # 30% 年化波动率
            risk_score += 1
        if price_change < -0.1:  # 10% 回撤
            risk_score += 1
            
        # 确定风险信号
        if risk_score >= 2:
            signal = "high_risk"
            confidence = 0.9
            max_position_size *= 0.5  # 高风险时减半仓位
        elif risk_score == 1:
            signal = "moderate_risk"
            confidence = 0.6
            max_position_size *= 0.7  # 中等风险时减少 30% 仓位
        else:
            signal = "low_risk"
            confidence = 0.7
        
        risk_assessment = {
            "signal": signal,
            "confidence": confidence,
            "metrics": {
                "current_price": current_price,
                "price_change": price_change,
                "volatility": volatility,
                "risk_score": risk_score
            },
            "max_position_size": max_position_size  # 添加最大仓位大小
        }
        
        message = HumanMessage(
            content=json.dumps(risk_assessment),
            name="risk_management_agent",
        )
        
        if state["metadata"]["show_reasoning"]:
            show_agent_reasoning(risk_assessment, "Risk Management Analysis")
        
        state["data"]["analyst_signals"]["risk_management_agent"] = risk_assessment
        
        return {
            "messages": [message],
            "data": data,
        }
        
    except Exception as e:
        print(f"Warning in risk management agent: {e}")
        # 返回默认的低风险信号
        default_result = {
            "signal": "low_risk",
            "confidence": 0.5,
            "max_position_size": 0.1,  # 默认最大仓位 10%
            "reasoning": "Error processing risk metrics"
        }
        
        message = HumanMessage(
            content=json.dumps(default_result),
            name="risk_management_agent",
        )
        
        state["data"]["analyst_signals"]["risk_management_agent"] = default_result
        
        return {
            "messages": [message],
            "data": data,
        }