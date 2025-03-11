import sys
import os
import json

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import questionary

import matplotlib.pyplot as plt
import pandas as pd
from colorama import Fore, Style, init
import numpy as np
import itertools

from llm.models import LLM_ORDER, get_model_info
from utils.analysts import ANALYST_CONFIG
from main import run_hedge_fund
from backtester import Backtester
from tools.api import get_price_data
from utils.display import print_backtest_results, format_backtest_row

init(autoreset=True)

# 定义两个agent组
FUNDAMENTALS_DRIVEN_AGENTS = [
    "ben_graham", "warren_buffett", "charlie_munger", 
    "bill_ackman", "valuation_analyst", "fundamentals_analyst"
]

DYNAMIC_SIGNAL_AGENTS = [
    "cathie_wood", "stanley_druckenmiller", 
    "sentiment_analyst", "technical_analyst"
]

# 定义组名和描述
AGENT_GROUPS = {
    "fundamentals_driven": {
        "display_name": "价值驱动型 (Fundamentals-Driven Agents)",
        "description": "包含: Ben Graham, Warren Buffett, Charlie Munger, Bill Ackman, Valuation Agent, Fundamentals Agent",
        "agents": FUNDAMENTALS_DRIVEN_AGENTS
    },
    "dynamic_signal": {
        "display_name": "变量捕捉型 (Dynamic Signal Agents)",
        "description": "包含: Cathie Wood, Stanley Druckenmiller, Sentiment Agent, Technicals Agent",
        "agents": DYNAMIC_SIGNAL_AGENTS
    }
}

def save_backtest_results(table_rows, performance_metrics, save_path, output_format="csv", tickers=None, start_date=None, end_date=None, performance_df=None):
    """
    保存回测结果到文件
    
    Args:
        table_rows (list): 回测结果表格行
        performance_metrics (dict): 性能指标
        save_path (str): 保存路径
        output_format (str): 输出格式 (csv, json, both)
        tickers (list): 股票代码列表
        start_date (str): 开始日期
        end_date (str): 结束日期
        performance_df (DataFrame): 性能数据框，用于绘制图表
    """
    # 创建时间戳文件夹
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 如果save_path是目录，则在其中创建时间戳文件夹
    if os.path.isdir(save_path) or not os.path.exists(save_path):
        save_dir = os.path.join(save_path, f"backtest_{timestamp}")
        os.makedirs(save_dir, exist_ok=True)
    else:
        # 如果save_path是文件路径，则使用该路径的目录
        save_dir = os.path.dirname(save_path)
        os.makedirs(save_dir, exist_ok=True)
    
    # 提取交易记录和摘要
    trade_records = []
    summary_records = []
    
    for row in table_rows:
        # 移除颜色代码
        clean_row = []
        for item in row:
            if isinstance(item, str):
                # 移除ANSI颜色代码
                for color_code in [Fore.GREEN, Fore.RED, Fore.YELLOW, Fore.CYAN, Fore.WHITE, Fore.BLUE, Style.BRIGHT, Style.RESET_ALL]:
                    item = item.replace(str(color_code), "")
            clean_row.append(item)
        
        # 区分交易记录和摘要
        if len(clean_row) > 1 and isinstance(clean_row[1], str) and "PORTFOLIO SUMMARY" in clean_row[1]:
            summary_records.append(clean_row)
        else:
            trade_records.append(clean_row)
    
    # 创建交易记录DataFrame
    if trade_records:
        trade_df = pd.DataFrame(trade_records, columns=[
            "Date", "Ticker", "Action", "Quantity", "Price", "Shares", 
            "Position Value", "Bullish", "Bearish", "Neutral"
        ])
    else:
        trade_df = pd.DataFrame()
    
    # 创建摘要DataFrame
    if summary_records:
        summary_df = pd.DataFrame(summary_records, columns=[
            "Date", "Summary", "Action", "Quantity", "Price", "Shares", 
            "Total Position Value", "Cash Balance", "Total Value", "Return", 
            "Sharpe Ratio", "Sortino Ratio", "Max Drawdown"
        ])
    else:
        summary_df = pd.DataFrame()
    
    # 保存为CSV
    if output_format in ["csv", "both"]:
        if not trade_df.empty:
            trade_csv_path = os.path.join(save_dir, "trade_records.csv")
            trade_df.to_csv(trade_csv_path, index=False)
            print(f"交易记录已保存到: {trade_csv_path}")
        
        if not summary_df.empty:
            summary_csv_path = os.path.join(save_dir, "portfolio_summary.csv")
            summary_df.to_csv(summary_csv_path, index=False)
            print(f"投资组合摘要已保存到: {summary_csv_path}")
    
    # 保存为JSON
    if output_format in ["json", "both"]:
        # 创建完整的结果字典
        result_dict = {
            "metadata": {
                "tickers": tickers,
                "start_date": start_date,
                "end_date": end_date,
                "timestamp": timestamp
            },
            "trade_records": trade_df.to_dict(orient="records") if not trade_df.empty else [],
            "portfolio_summary": summary_df.to_dict(orient="records") if not summary_df.empty else [],
            "performance_metrics": performance_metrics
        }
        
        json_path = os.path.join(save_dir, "backtest_results.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False)
        print(f"完整回测结果已保存到: {json_path}")
    
    # 保存图表
    if performance_df is not None and not performance_df.empty:
        # 创建投资组合价值随时间变化的图表
        plt.figure(figsize=(12, 6))
        plt.plot(performance_df.index, performance_df["Portfolio Value"], color="blue")
        plt.title("Portfolio Value Over Time")
        plt.ylabel("Portfolio Value ($)")
        plt.xlabel("Date")
        plt.grid(True)
        
        # 保存图表
        chart_path = os.path.join(save_dir, "portfolio_value_chart.png")
        plt.savefig(chart_path, dpi=300, bbox_inches="tight")
        plt.close()  # 关闭图表，避免显示
        print(f"投资组合价值图表已保存到: {chart_path}")
    
    return save_dir


class BacktesterGroup:
    def __init__(
        self,
        agent_group: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        initial_capital: float,
        model_name: str = "gpt-4o",
        model_provider: str = "OpenAI",
        initial_margin_requirement: float = 0.0,
        disable_short_positions: bool = False,
    ):
        """
        :param agent_group: 选择的agent组 ('fundamentals_driven' 或 'dynamic_signal')
        :param tickers: 要回测的股票代码列表
        :param start_date: 开始日期字符串 (YYYY-MM-DD)
        :param end_date: 结束日期字符串 (YYYY-MM-DD)
        :param initial_capital: 初始资金
        :param model_name: 使用的LLM模型名称 (gpt-4, 等)
        :param model_provider: 使用的LLM提供商 (OpenAI, 等)
        :param initial_margin_requirement: 保证金比例 (例如 0.5 = 50%)
        :param disable_short_positions: 是否禁用做空操作
        """
        self.agent_group = agent_group
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.model_name = model_name
        self.model_provider = model_provider
        self.initial_margin_requirement = initial_margin_requirement
        self.disable_short_positions = disable_short_positions
        
        # 获取选定组中的所有agent
        self.selected_analysts = AGENT_GROUPS[agent_group]["agents"]
        
        # 创建Backtester实例
        self.backtester = Backtester(
            agent=run_hedge_fund,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            model_name=model_name,
            model_provider=model_provider,
            selected_analysts=self.selected_analysts,
            initial_margin_requirement=initial_margin_requirement,
            disable_short_positions=disable_short_positions,
        )
    
    def run_backtest(self):
        """运行回测"""
        return self.backtester.run_backtest()
    
    def analyze_performance(self):
        """分析回测性能"""
        return self.backtester.analyze_performance()


### 运行回测 ###
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="运行分组回测模拟")
    parser.add_argument(
        "--tickers",
        type=str,
        required=False,
        help="股票代码列表，用逗号分隔 (例如, AAPL,MSFT,GOOGL)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="结束日期，格式为 YYYY-MM-DD",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=(datetime.now() - relativedelta(months=1)).strftime("%Y-%m-%d"),
        help="开始日期，格式为 YYYY-MM-DD",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=100000,
        help="初始资金金额 (默认: 100000)",
    )
    parser.add_argument(
        "--margin-requirement",
        type=float,
        default=0.0,
        help="做空保证金比例，例如 0.5 表示 50% (默认: 0.0)",
    )
    parser.add_argument(
        "--disable-short-positions",
        action="store_true",
        help="禁用做空和平仓操作，只允许买入、卖出和持有",
    )
    parser.add_argument(
        "--save-results",
        type=str,
        default="",
        help="保存回测结果的路径 (默认不保存)",
    )
    parser.add_argument(
        "--output-format",
        type=str,
        choices=["csv", "json", "both"],
        default="csv",
        help="输出格式: csv, json 或 both (默认: csv)",
    )

    args = parser.parse_args()

    # 从逗号分隔的字符串解析股票代码
    tickers = [ticker.strip() for ticker in args.tickers.split(",")] if args.tickers else []

    # 选择agent组
    choices = questionary.select(
        "请选择要使用的分析师组:",
        choices=[
            questionary.Choice(
                f"{group_info['display_name']}\n  {group_info['description']}",
                value=group_key
            ) for group_key, group_info in AGENT_GROUPS.items()
        ],
        style=questionary.Style([
            ("selected", "fg:green bold"),
            ("pointer", "fg:green bold"),
            ("highlighted", "fg:green"),
            ("answer", "fg:green bold"),
        ])
    ).ask()

    if not choices:
        print("\n\n收到中断信号。退出...")
        sys.exit(0)
    else:
        selected_group = choices
        group_info = AGENT_GROUPS[selected_group]
        print(f"\n已选择分析师组: {Fore.GREEN}{group_info['display_name']}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{group_info['description']}{Style.RESET_ALL}\n")

    # 选择LLM模型
    model_choice = questionary.select(
        "选择您的LLM模型:",
        choices=[questionary.Choice(display, value=value) for display, value, _ in LLM_ORDER],
        style=questionary.Style([
            ("selected", "fg:green bold"),
            ("pointer", "fg:green bold"),
            ("highlighted", "fg:green"),
            ("answer", "fg:green bold"),
        ])
    ).ask()

    if not model_choice:
        print("\n\n收到中断信号。退出...")
        sys.exit(0)
    else:
        model_info = get_model_info(model_choice)
        if model_info:
            model_provider = model_info.provider.value
            print(f"\n已选择 {Fore.CYAN}{model_provider}{Style.RESET_ALL} 模型: {Fore.GREEN + Style.BRIGHT}{model_choice}{Style.RESET_ALL}\n")
        else:
            model_provider = "Unknown"
            print(f"\n已选择模型: {Fore.GREEN + Style.BRIGHT}{model_choice}{Style.RESET_ALL}\n")

    # 创建并运行回测器
    backtester_group = BacktesterGroup(
        agent_group=selected_group,
        tickers=tickers,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_capital=args.initial_capital,
        model_name=model_choice,
        model_provider=model_provider,
        initial_margin_requirement=args.margin_requirement,
        disable_short_positions=args.disable_short_positions,
    )

    performance_metrics = backtester_group.run_backtest()
    performance_df = backtester_group.analyze_performance()
    
    # 如果指定了保存路径，则保存回测结果
    if args.save_results:
        # 重新运行回测以获取表格行数据
        # 由于backtester.py中的table_rows是局部变量，我们需要手动重建它
        
        # 创建交易记录
        trade_records = []
        for ticker in tickers:
            position = backtester_group.backtester.portfolio["positions"][ticker]
            # 获取最新价格
            try:
                current_date_str = args.end_date
                previous_date_str = (datetime.strptime(args.end_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                current_price = get_price_data(ticker, previous_date_str, current_date_str).iloc[-1]["close"]
            except:
                current_price = 0
                
            # 计算持仓价值
            long_val = position["long"] * current_price if current_price > 0 else 0
            short_val = position["short"] * current_price if current_price > 0 else 0
            net_position_value = long_val - short_val
            
            # 添加开始日期的记录
            trade_records.append(
                format_backtest_row(
                    date=args.start_date,
                    ticker=ticker,
                    action="HOLD" if position["long"] == 0 and position["short"] == 0 else 
                           "LONG" if position["long"] > 0 else "SHORT",
                    quantity=max(position["long"], position["short"]),
                    price=current_price,
                    shares_owned=position["long"] - position["short"],
                    position_value=net_position_value,
                    bullish_count=0,
                    bearish_count=0,
                    neutral_count=0,
                )
            )
            
            # 添加结束日期的记录
            trade_records.append(
                format_backtest_row(
                    date=args.end_date,
                    ticker=ticker,
                    action="HOLD",
                    quantity=0,
                    price=current_price,
                    shares_owned=position["long"] - position["short"],
                    position_value=net_position_value,
                    bullish_count=0,
                    bearish_count=0,
                    neutral_count=0,
                )
            )
        
        # 添加投资组合摘要
        final_portfolio_value = backtester_group.backtester.calculate_portfolio_value({
            ticker: get_price_data(ticker, 
                                  (datetime.strptime(args.end_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"), 
                                  args.end_date).iloc[-1]["close"] 
            for ticker in tickers
        }) if tickers else backtester_group.backtester.portfolio["cash"]
        
        total_position_value = final_portfolio_value - backtester_group.backtester.portfolio["cash"]
        portfolio_return = ((final_portfolio_value / backtester_group.backtester.initial_capital) - 1) * 100
        
        trade_records.append(
            format_backtest_row(
                date=args.end_date,
                ticker="",
                action="",
                quantity=0,
                price=0,
                shares_owned=0,
                position_value=0,
                bullish_count=0,
                bearish_count=0,
                neutral_count=0,
                is_summary=True,
                total_value=final_portfolio_value,
                return_pct=portfolio_return,
                cash_balance=backtester_group.backtester.portfolio["cash"],
                total_position_value=total_position_value,
                sharpe_ratio=performance_metrics.get('sharpe_ratio'),
                sortino_ratio=performance_metrics.get('sortino_ratio'),
                max_drawdown=performance_metrics.get('max_drawdown'),
            )
        )
        
        save_dir = save_backtest_results(
            table_rows=trade_records,
            performance_metrics=performance_metrics,
            save_path=args.save_results,
            output_format=args.output_format,
            tickers=tickers,
            start_date=args.start_date,
            end_date=args.end_date,
            performance_df=performance_df
        )
        print(f"\n{Fore.GREEN}回测结果已保存到: {save_dir}{Style.RESET_ALL}")
