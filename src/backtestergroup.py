import sys

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
