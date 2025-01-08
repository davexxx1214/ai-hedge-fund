import math
from langchain_core.messages import HumanMessage
from graph.state import AgentState, show_agent_reasoning
import json
import pandas as pd
import numpy as np
from tools.api import get_prices

##### Technical Analyst #####
def technical_analyst_agent(state: AgentState):
    data = state["data"]
    
    # 获取价格数据
    prices_df = get_prices(
        ticker=data["ticker"],
        start_date=data["start_date"],
        end_date=data["end_date"],
    )
    
    # 处理多级索引列
    if isinstance(prices_df.columns, pd.MultiIndex):
        # 只保留第一级索引的值
        prices_df.columns = prices_df.columns.get_level_values(0)
    
    # 将列名转换为小写
    prices_df.columns = prices_df.columns.str.lower()
    
    # 检查数据是否为空
    if prices_df.empty:
        raise ValueError("No price data available")
        
    # 确保所需的列都存在
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    if not all(col in prices_df.columns for col in required_columns):
        raise ValueError(f"Missing required columns. Found columns: {prices_df.columns.tolist()}")
    
    # 计算各种技术指标信号
    trend_signals = calculate_trend_signals(prices_df)
    mean_reversion_signals = calculate_mean_reversion_signals(prices_df)
    momentum_signals = calculate_momentum_signals(prices_df)
    volatility_signals = calculate_volatility_signals(prices_df)
    stat_arb_signals = calculate_stat_arb_signals(prices_df)

    # 使用加权集成方法组合所有信号
    strategy_weights = {
        "trend": 0.25,
        "mean_reversion": 0.20,
        "momentum": 0.25,
        "volatility": 0.15,
        "stat_arb": 0.15,
    }

    combined_signal = weighted_signal_combination(
        {
            "trend": trend_signals,
            "mean_reversion": mean_reversion_signals,
            "momentum": momentum_signals,
            "volatility": volatility_signals,
            "stat_arb": stat_arb_signals,
        },
        strategy_weights,
    )

    # 生成分析报告
    analysis_report = {
        "signal": combined_signal["signal"],
        "confidence": round(combined_signal["confidence"] * 100),
        "strategy_signals": {
            "trend_following": {
                "signal": trend_signals["signal"],
                "confidence": round(trend_signals["confidence"] * 100),
                "metrics": normalize_pandas(trend_signals["metrics"]),
            },
            "mean_reversion": {
                "signal": mean_reversion_signals["signal"],
                "confidence": round(mean_reversion_signals["confidence"] * 100),
                "metrics": normalize_pandas(mean_reversion_signals["metrics"]),
            },
            "momentum": {
                "signal": momentum_signals["signal"],
                "confidence": round(momentum_signals["confidence"] * 100),
                "metrics": normalize_pandas(momentum_signals["metrics"]),
            },
            "volatility": {
                "signal": volatility_signals["signal"],
                "confidence": round(volatility_signals["confidence"] * 100),
                "metrics": normalize_pandas(volatility_signals["metrics"]),
            },
            "statistical_arbitrage": {
                "signal": stat_arb_signals["signal"],
                "confidence": round(stat_arb_signals["confidence"] * 100),
                "metrics": normalize_pandas(stat_arb_signals["metrics"]),
            },
        },
    }

    # 创建技术分析消息
    message = HumanMessage(
        content=json.dumps(analysis_report),
        name="technical_analyst_agent",
    )

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(analysis_report, "Technical Analyst")

    # 将信号添加到 analyst_signals 列表
    state["data"]["analyst_signals"]["technical_analyst_agent"] = {
        "signal": analysis_report["signal"],
        "confidence": analysis_report["confidence"],
        "reasoning": analysis_report["strategy_signals"],
    }

    return {
        "messages": state["messages"] + [message],
        "data": data,
    }

def normalize_pandas(d):
    """将 pandas 对象转换为 JSON 可序列化的格式"""
    result = {}
    for k, v in d.items():
        if isinstance(v, (pd.Series, pd.DataFrame)):
            result[k] = float(v.iloc[-1])
        elif isinstance(v, (np.integer, np.floating)):
            result[k] = float(v)
        elif isinstance(v, (int, float)):
            result[k] = v
        else:
            result[k] = str(v)
    return result

def calculate_ema(df, window):
    """计算指数移动平均线"""
    if 'close' not in df.columns:
        raise KeyError("No 'close' price column found in the dataframe")
    return df['close'].ewm(span=window, adjust=False).mean()

def calculate_adx(df, period=14):
    """计算 ADX (Average Directional Index)"""
    if not all(col in df.columns for col in ['high', 'low', 'close']):
        raise KeyError("Missing required price columns (high, low, close)")
        
    # 计算 +DM 和 -DM
    high_diff = df['high'].diff()
    low_diff = -df['low'].diff()
    
    plus_dm = high_diff.where((high_diff > low_diff) & (high_diff > 0), 0)
    minus_dm = low_diff.where((low_diff > high_diff) & (low_diff > 0), 0)
    
    # 计算 TR
    tr = pd.DataFrame({
        'hl': df['high'] - df['low'],
        'hc': abs(df['high'] - df['close'].shift(1)),
        'lc': abs(df['low'] - df['close'].shift(1))
    }).max(axis=1)
    
    # 计算平滑值
    tr_ema = tr.ewm(span=period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(span=period, adjust=False).mean() / tr_ema)
    minus_di = 100 * (minus_dm.ewm(span=period, adjust=False).mean() / tr_ema)
    
    # 计算 ADX
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.ewm(span=period, adjust=False).mean()
    
    return pd.DataFrame({
        'plus_di': plus_di,
        'minus_di': minus_di,
        'adx': adx
    })

def calculate_trend_signals(prices_df):
    """高级趋势跟踪策略，使用多个时间框架和指标"""
    # 计算多个时间框架的 EMA
    ema_8 = calculate_ema(prices_df, 8)
    ema_21 = calculate_ema(prices_df, 21)
    ema_55 = calculate_ema(prices_df, 55)

    # 计算趋势强度的 ADX
    adx = calculate_adx(prices_df, 14)

    # 确定趋势方向和强度
    short_trend = ema_8 > ema_21
    medium_trend = ema_21 > ema_55

    # 结合信号和置信度权重
    trend_strength = adx["adx"].iloc[-1] / 100.0

    if short_trend.iloc[-1] and medium_trend.iloc[-1]:
        signal = "bullish"
        confidence = trend_strength
    elif not short_trend.iloc[-1] and not medium_trend.iloc[-1]:
        signal = "bearish"
        confidence = trend_strength
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "adx": adx["adx"].iloc[-1],
            "trend_strength": trend_strength,
        },
    }

def calculate_bollinger_bands(df, window=20, num_std=2):
    """计算布林带"""
    if 'close' not in df.columns:
        raise KeyError("No 'close' price column found in the dataframe")
        
    ma = df['close'].rolling(window=window).mean()
    std = df['close'].rolling(window=window).std()
    upper = ma + (std * num_std)
    lower = ma - (std * num_std)
    return upper, lower

def calculate_rsi(df, period=14):
    """计算相对强弱指标 (RSI)"""
    if 'close' not in df.columns:
        raise KeyError("No 'close' price column found in the dataframe")
        
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_mean_reversion_signals(prices_df):
    """使用统计测度和布林带的均值回归策略"""
    # 计算相对于移动平均线的 z-score
    ma_50 = prices_df['close'].rolling(window=50).mean()
    std_50 = prices_df['close'].rolling(window=50).std()
    z_score = (prices_df['close'] - ma_50) / std_50

    # 计算布林带
    bb_upper, bb_lower = calculate_bollinger_bands(prices_df)

    # 计算多个时间框架的 RSI
    rsi_14 = calculate_rsi(prices_df, 14)
    rsi_28 = calculate_rsi(prices_df, 28)

    # 价格相对于布林带的位置
    price_vs_bb = (prices_df['close'].iloc[-1] - bb_lower.iloc[-1]) / (
        bb_upper.iloc[-1] - bb_lower.iloc[-1]
    )

    # 组合信号
    if z_score.iloc[-1] < -2 and price_vs_bb < 0.2:
        signal = "bullish"
        confidence = min(abs(z_score.iloc[-1]) / 4, 1.0)
    elif z_score.iloc[-1] > 2 and price_vs_bb > 0.8:
        signal = "bearish"
        confidence = min(abs(z_score.iloc[-1]) / 4, 1.0)
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "z_score": z_score.iloc[-1],
            "price_vs_bb": price_vs_bb,
            "rsi_14": rsi_14.iloc[-1],
            "rsi_28": rsi_28.iloc[-1],
        },
    }

def calculate_momentum_signals(prices_df):
    """多因子动量策略"""
    # 价格动量
    returns = prices_df['close'].pct_change()
    mom_1m = returns.rolling(21).sum()
    mom_3m = returns.rolling(63).sum()
    mom_6m = returns.rolling(126).sum()

    # 成交量动量
    volume_ma = prices_df['volume'].rolling(21).mean()
    volume_momentum = prices_df['volume'] / volume_ma

    # 计算动量得分
    momentum_score = (0.4 * mom_1m + 0.3 * mom_3m + 0.3 * mom_6m).iloc[-1]

    # 成交量确认
    volume_confirmation = volume_momentum.iloc[-1] > 1.0

    if momentum_score > 0.05 and volume_confirmation:
        signal = "bullish"
        confidence = min(abs(momentum_score) * 5, 1.0)
    elif momentum_score < -0.05 and volume_confirmation:
        signal = "bearish"
        confidence = min(abs(momentum_score) * 5, 1.0)
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "momentum_1m": mom_1m.iloc[-1],
            "momentum_3m": mom_3m.iloc[-1],
            "momentum_6m": mom_6m.iloc[-1],
            "volume_momentum": volume_momentum.iloc[-1],
        },
    }

def calculate_atr(df, period=14):
    """计算平均真实范围 (ATR)"""
    if not all(col in df.columns for col in ['high', 'low', 'close']):
        raise KeyError("Missing required price columns (high, low, close)")
        
    tr = pd.DataFrame({
        'hl': df['high'] - df['low'],
        'hc': abs(df['high'] - df['close'].shift(1)),
        'lc': abs(df['low'] - df['close'].shift(1))
    }).max(axis=1)
    
    return tr.ewm(span=period, adjust=False).mean()

def calculate_volatility_signals(prices_df):
    """基于波动率的交易策略"""
    # 计算各种波动率指标
    returns = prices_df['close'].pct_change()

    # 历史波动率
    hist_vol = returns.rolling(21).std() * math.sqrt(252)

    # 波动率制度检测
    vol_ma = hist_vol.rolling(63).mean()
    vol_regime = hist_vol / vol_ma

    # 波动率均值回归
    vol_z_score = (hist_vol - vol_ma) / hist_vol.rolling(63).std()

    # ATR 比率
    atr = calculate_atr(prices_df)
    atr_ratio = atr / prices_df['close']

    # 基于波动率制度生成信号
    current_vol_regime = vol_regime.iloc[-1]
    vol_z = vol_z_score.iloc[-1]

    if current_vol_regime < 0.8 and vol_z < -1:
        signal = "bullish"  # 低波动率制度，可能扩张
        confidence = min(abs(vol_z) / 3, 1.0)
    elif current_vol_regime > 1.2 and vol_z > 1:
        signal = "bearish"  # 高波动率制度，可能收缩
        confidence = min(abs(vol_z) / 3, 1.0)
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "historical_volatility": hist_vol.iloc[-1],
            "volatility_regime": current_vol_regime,
            "volatility_z_score": vol_z,
            "atr_ratio": atr_ratio.iloc[-1],
        },
    }

def calculate_hurst_exponent(series, lags=range(2, 100)):
    """计算赫斯特指数"""
    # 将传入的系列转换为 numpy 数组
    series = np.array(series)
    
    # 计算每个时间滞后的 R/S 比率
    rs_values = []
    for lag in lags:
        # 计算回报率
        ret = np.diff(np.log(series))
        # 计算均值调整后的回报
        mean_adj_ret = ret - ret.mean()
        # 计算累积偏差
        cum_dev = mean_adj_ret.cumsum()
        # 计算极差 R
        r = cum_dev.max() - cum_dev.min()
        # 计算标准差 S
        s = ret.std()
        if s != 0:  # 避免除以零
            rs_values.append(r/s)
    
    # 计算 Hurst 指数（使用对数回归）
    if len(rs_values) > 0:
        lags_log = np.log(lags)
        rs_log = np.log(rs_values)
        hurst = np.polyfit(lags_log, rs_log, 1)[0]
        return hurst
    return 0.5  # 如果无法计算，返回随机游走的 Hurst 指数

def calculate_stat_arb_signals(prices_df):
    """统计套利信号生成器"""
    if len(prices_df) < 100:  # 确保有足够的数据点
        return {
            "signal": "neutral",
            "confidence": 0.5,
            "metrics": {
                "hurst_exponent": 0.5,
                "zscore": 0,
                "mean_reversion_strength": 0
            }
        }
    
    # 计算赫斯特指数
    prices = prices_df['close'].values
    hurst = calculate_hurst_exponent(prices)
    
    # 计算价格的 z-score
    rolling_mean = prices_df['close'].rolling(window=20).mean()
    rolling_std = prices_df['close'].rolling(window=20).std()
    zscore = (prices_df['close'] - rolling_mean) / rolling_std
    current_zscore = zscore.iloc[-1]
    
    # 计算均值回归强度
    mean_reversion_strength = 1 - hurst  # 转换为均值回归强度
    
    # 生成信号
    if hurst < 0.4 and abs(current_zscore) > 2:  # 强均值回归特征
        signal = "bullish" if current_zscore < -2 else "bearish"
        confidence = min(abs(current_zscore) / 4, 1.0) * mean_reversion_strength
    else:
        signal = "neutral"
        confidence = 0.5
    
    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "hurst_exponent": hurst,
            "zscore": current_zscore,
            "mean_reversion_strength": mean_reversion_strength
        }
    }

def weighted_signal_combination(signals, weights):
    """
    使用加权方法组合多个交易信号
    
    Args:
        signals: 包含各个策略信号的字典
        weights: 各个策略的权重字典
    
    Returns:
        dict: 组合后的信号和置信度
    """
    bullish_score = 0
    bearish_score = 0
    total_weight = sum(weights.values())
    
    # 标准化权重
    weights = {k: w/total_weight for k, w in weights.items()}
    
    # 计算加权得分
    for strategy, signal_data in signals.items():
        weight = weights[strategy]
        if signal_data["signal"] == "bullish":
            bullish_score += weight * signal_data["confidence"]
        elif signal_data["signal"] == "bearish":
            bearish_score += weight * signal_data["confidence"]
    
    # 确定最终信号
    if bullish_score > bearish_score:
        return {
            "signal": "bullish",
            "confidence": bullish_score / (bullish_score + bearish_score)
        }
    elif bearish_score > bullish_score:
        return {
            "signal": "bearish",
            "confidence": bearish_score / (bullish_score + bearish_score)
        }
    else:
        return {
            "signal": "neutral",
            "confidence": 0.5
        }