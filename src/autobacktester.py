import sys

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import argparse

import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
from colorama import Fore, Style, init
import numpy as np
import itertools

# 设置matplotlib支持中文
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

from src.llm.models import LLM_ORDER, OLLAMA_LLM_ORDER, get_model_info, ModelProvider
from src.utils.analysts import ANALYST_ORDER
from src.main import run_hedge_fund
from src.tools.api import (
    get_company_news,
    get_price_data,
    get_prices,
    get_financial_metrics,
    get_insider_trades,
)
from src.utils.display import print_backtest_results, format_backtest_row
from typing_extensions import Callable
from src.utils.ollama import ensure_ollama_and_model

init(autoreset=True)


class AutoBacktester:
    def __init__(
        self,
        agent: Callable,
        tickers: list[str],
        start_date: str,
        end_date: str,
        initial_capital: float,
        model_name: str = "gpt-4o-mini",
        model_provider: str = "OpenAI",
        selected_analysts: list[str] = None,
        initial_margin_requirement: float = 0.0,
    ):
        """
        自动回测器，采用 Buy and Hold 策略
        
        :param agent: 交易代理 (Callable).
        :param tickers: 要回测的股票代码列表.
        :param start_date: 开始日期字符串 (YYYY-MM-DD).
        :param end_date: 结束日期字符串 (YYYY-MM-DD).
        :param initial_capital: 初始资金.
        :param model_name: 使用的LLM模型名称.
        :param model_provider: LLM提供商.
        :param selected_analysts: 分析师名称列表.
        :param initial_margin_requirement: 保证金比例 (例如 0.5 = 50%).
        """
        self.agent = agent
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.model_name = model_name
        self.model_provider = model_provider
        
        # 自动选择指定的 analysts
        if selected_analysts is None:
            self.selected_analysts = ["ben_graham", "charlie_munger", "warren_buffett"]
        else:
            self.selected_analysts = selected_analysts

        # 初始化投资组合
        self.portfolio_values = []
        self.portfolio = {
            "cash": initial_capital,
            "margin_used": 0.0,
            "margin_requirement": initial_margin_requirement,
            "positions": {ticker: {"long": 0, "short": 0, "long_cost_basis": 0.0, "short_cost_basis": 0.0, "short_margin_used": 0.0} for ticker in tickers},
            "realized_gains": {
                ticker: {
                    "long": 0.0,
                    "short": 0.0,
                }
                for ticker in tickers
            },
        }

    def execute_trade(self, ticker: str, action: str, quantity: float, current_price: float):
        """
        执行交易，支持多头和空头头寸
        """
        if quantity <= 0:
            return 0

        quantity = int(quantity)  # 强制整数股数
        position = self.portfolio["positions"][ticker]

        if action == "buy":
            cost = quantity * current_price
            if cost <= self.portfolio["cash"]:
                # 加权平均成本基础
                old_shares = position["long"]
                old_cost_basis = position["long_cost_basis"]
                new_shares = quantity
                total_shares = old_shares + new_shares

                if total_shares > 0:
                    total_old_cost = old_cost_basis * old_shares
                    total_new_cost = cost
                    position["long_cost_basis"] = (total_old_cost + total_new_cost) / total_shares

                position["long"] += quantity
                self.portfolio["cash"] -= cost
                return quantity
            else:
                # 计算最大可负担数量
                max_quantity = int(self.portfolio["cash"] / current_price)
                if max_quantity > 0:
                    cost = max_quantity * current_price
                    old_shares = position["long"]
                    old_cost_basis = position["long_cost_basis"]
                    total_shares = old_shares + max_quantity

                    if total_shares > 0:
                        total_old_cost = old_cost_basis * old_shares
                        total_new_cost = cost
                        position["long_cost_basis"] = (total_old_cost + total_new_cost) / total_shares

                    position["long"] += max_quantity
                    self.portfolio["cash"] -= cost
                    return max_quantity
                return 0

        elif action == "sell":
            # 只能卖出拥有的股票
            quantity = min(quantity, position["long"])
            if quantity > 0:
                # 使用平均成本基础计算已实现收益/损失
                avg_cost_per_share = position["long_cost_basis"] if position["long"] > 0 else 0
                realized_gain = (current_price - avg_cost_per_share) * quantity
                self.portfolio["realized_gains"][ticker]["long"] += realized_gain

                position["long"] -= quantity
                self.portfolio["cash"] += quantity * current_price

                if position["long"] == 0:
                    position["long_cost_basis"] = 0.0

                return quantity

        elif action == "short":
            """
            Typical short sale flow:
              1) Receive proceeds = current_price * quantity
              2) Post margin_required = proceeds * margin_ratio
              3) Net effect on cash = +proceeds - margin_required
            """
            proceeds = current_price * quantity
            margin_required = proceeds * self.portfolio["margin_requirement"]
            if margin_required <= self.portfolio["cash"]:
                # Weighted average short cost basis
                old_short_shares = position["short"]
                old_cost_basis = position["short_cost_basis"]
                new_shares = quantity
                total_shares = old_short_shares + new_shares

                if total_shares > 0:
                    total_old_cost = old_cost_basis * old_short_shares
                    total_new_cost = current_price * new_shares
                    position["short_cost_basis"] = (total_old_cost + total_new_cost) / total_shares

                position["short"] += quantity

                # Update margin usage
                position["short_margin_used"] += margin_required
                self.portfolio["margin_used"] += margin_required

                # Increase cash by proceeds, then subtract the required margin
                self.portfolio["cash"] += proceeds
                self.portfolio["cash"] -= margin_required
                return quantity
            else:
                # Calculate maximum shortable quantity
                margin_ratio = self.portfolio["margin_requirement"]
                if margin_ratio > 0:
                    max_quantity = int(self.portfolio["cash"] / (current_price * margin_ratio))
                else:
                    max_quantity = 0

                if max_quantity > 0:
                    proceeds = current_price * max_quantity
                    margin_required = proceeds * margin_ratio

                    old_short_shares = position["short"]
                    old_cost_basis = position["short_cost_basis"]
                    total_shares = old_short_shares + max_quantity

                    if total_shares > 0:
                        total_old_cost = old_cost_basis * old_short_shares
                        total_new_cost = current_price * max_quantity
                        position["short_cost_basis"] = (total_old_cost + total_new_cost) / total_shares

                    position["short"] += max_quantity
                    position["short_margin_used"] += margin_required
                    self.portfolio["margin_used"] += margin_required

                    self.portfolio["cash"] += proceeds
                    self.portfolio["cash"] -= margin_required
                    return max_quantity
                return 0

        elif action == "cover":
            """
            When covering shares:
              1) Pay cover cost = current_price * quantity
              2) Release a proportional share of the margin
              3) Net effect on cash = -cover_cost + released_margin
            """
            quantity = min(quantity, position["short"])
            if quantity > 0:
                cover_cost = quantity * current_price
                avg_short_price = position["short_cost_basis"] if position["short"] > 0 else 0
                realized_gain = (avg_short_price - current_price) * quantity

                if position["short"] > 0:
                    portion = quantity / position["short"]
                else:
                    portion = 1.0

                margin_to_release = portion * position["short_margin_used"]

                position["short"] -= quantity
                position["short_margin_used"] -= margin_to_release
                self.portfolio["margin_used"] -= margin_to_release

                # Pay the cost to cover, but get back the released margin
                self.portfolio["cash"] += margin_to_release
                self.portfolio["cash"] -= cover_cost

                self.portfolio["realized_gains"][ticker]["short"] += realized_gain

                if position["short"] == 0:
                    position["short_cost_basis"] = 0.0
                    position["short_margin_used"] = 0.0

                return quantity

        return 0

    def calculate_portfolio_value(self, current_prices):
        """
        计算投资组合总价值，包括：
          - 现金
          - 多头头寸市场价值
          - 空头头寸的未实现收益/损失
        """
        total_value = self.portfolio["cash"]

        for ticker in self.tickers:
            position = self.portfolio["positions"][ticker]
            price = current_prices[ticker]

            # 多头头寸价值
            long_value = position["long"] * price
            total_value += long_value

            # 空头头寸未实现损益
            if position["short"] > 0:
                total_value -= position["short"] * price

        return total_value

    def prefetch_data(self):
        """预取回测期间所需的所有数据"""
        print("\n预取回测期间所需的所有数据...")

        # 转换结束日期为datetime，获取1年前的数据
        end_date_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        start_date_dt = end_date_dt - relativedelta(years=1)
        start_date_str = start_date_dt.strftime("%Y-%m-%d")

        for ticker in self.tickers:
            # 获取整个期间的价格数据，加上1年
            get_prices(ticker, start_date_str, self.end_date)

            # 获取财务指标
            get_financial_metrics(ticker, self.end_date, limit=10)

            # 获取内部人交易
            get_insider_trades(ticker, self.end_date, start_date=self.start_date, limit=1000)

            # 获取公司新闻
            get_company_news(ticker, self.end_date, start_date=self.start_date, limit=1000)

        print("数据预取完成。")

    def run_backtest(self):
        # 开始时预取所有数据
        self.prefetch_data()

        print(f"\n开始 Buy and Hold 回测...")
        print(f"选择的分析师: {', '.join([name.replace('_', ' ').title() for name in self.selected_analysts])}")
        print(f"选择的模型: {self.model_name} ({self.model_provider})")
        print(f"股票: {', '.join(self.tickers)}")
        print(f"开始日期: {self.start_date}")
        print(f"结束日期: {self.end_date}")
        print(f"初始资金: ${self.initial_capital:,.2f}")

        # ---------------------------------------------------------------
        # 1) 在开始日期执行一次策略决策
        # ---------------------------------------------------------------
        lookback_start = (datetime.strptime(self.start_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # 获取开始日期的价格
        try:
            start_prices = {}
            missing_data = False

            for ticker in self.tickers:
                try:
                    price_data = get_price_data(ticker, lookback_start, self.start_date)
                    if price_data.empty:
                        print(f"警告: {ticker} 在 {self.start_date} 没有价格数据")
                        missing_data = True
                        break
                    start_prices[ticker] = price_data.iloc[-1]["close"]
                except Exception as e:
                    print(f"获取 {ticker} 在 {self.start_date} 的价格时出错: {e}")
                    missing_data = True
                    break

            if missing_data:
                print(f"由于缺少价格数据，跳过 {self.start_date}")
                return {}

        except Exception as e:
            print(f"获取 {self.start_date} 价格时出错: {e}")
            return {}

        # 执行策略
        print(f"\n在 {self.start_date} 执行投资策略...")
        output = self.agent(
            tickers=self.tickers,
            start_date=lookback_start,
            end_date=self.start_date,
            portfolio=self.portfolio,
            model_name=self.model_name,
            model_provider=self.model_provider,
            selected_analysts=self.selected_analysts,
        )
        decisions = output["decisions"]
        analyst_signals = output["analyst_signals"]

        # 执行交易
        executed_trades = {}
        print(f"\n执行交易 ({self.start_date}):")
        for ticker in self.tickers:
            decision = decisions.get(ticker, {"action": "hold", "quantity": 0})
            action, quantity = decision.get("action", "hold"), decision.get("quantity", 0)

            executed_quantity = self.execute_trade(ticker, action, quantity, start_prices[ticker])
            executed_trades[ticker] = executed_quantity
            
            if executed_quantity > 0:
                print(f"  {ticker}: {action.upper()} {executed_quantity} 股 @ ${start_prices[ticker]:.2f}")
            else:
                print(f"  {ticker}: 无交易 (决策: {action})")

        # 记录初始投资后的投资组合状态
        initial_portfolio_value = self.calculate_portfolio_value(start_prices)
        self.portfolio_values.append({
            "Date": datetime.strptime(self.start_date, "%Y-%m-%d"),
            "Portfolio Value": initial_portfolio_value
        })

        print(f"\n初始投资后的投资组合价值: ${initial_portfolio_value:,.2f}")
        print(f"剩余现金: ${self.portfolio['cash']:,.2f}")
        
        for ticker in self.tickers:
            pos = self.portfolio["positions"][ticker]
            if pos["long"] > 0:
                value = pos["long"] * start_prices[ticker]
                print(f"{ticker}: {pos['long']} 股 (价值: ${value:,.2f})")

        # ---------------------------------------------------------------
        # 2) 在结束日期计算最终价值
        # ---------------------------------------------------------------
        print(f"\n计算 {self.end_date} 的最终投资组合价值...")
        
        try:
            end_prices = {}
            missing_data = False

            for ticker in self.tickers:
                try:
                    # 获取结束日期附近的价格范围，确保获得最接近的可用价格
                    end_date_start = (datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
                    price_data = get_price_data(ticker, end_date_start, self.end_date)
                    if price_data.empty:
                        print(f"警告: {ticker} 在 {self.end_date} 没有价格数据")
                        missing_data = True
                        break
                    end_prices[ticker] = price_data.iloc[-1]["close"]
                except Exception as e:
                    print(f"获取 {ticker} 在 {self.end_date} 的价格时出错: {e}")
                    missing_data = True
                    break

            if missing_data:
                print(f"由于缺少价格数据，无法计算最终价值")
                return {}

        except Exception as e:
            print(f"获取 {self.end_date} 价格时出错: {e}")
            return {}

        # 计算最终投资组合价值
        final_portfolio_value = self.calculate_portfolio_value(end_prices)
        self.portfolio_values.append({
            "Date": datetime.strptime(self.end_date, "%Y-%m-%d"),
            "Portfolio Value": final_portfolio_value
        })

        # 计算收益
        total_return = ((final_portfolio_value - self.initial_capital) / self.initial_capital) * 100
        absolute_return = final_portfolio_value - self.initial_capital

        print(f"\n{Fore.WHITE}{Style.BRIGHT}投资组合最终结果:{Style.RESET_ALL}")
        print(f"初始资金: ${self.initial_capital:,.2f}")
        print(f"最终价值: ${final_portfolio_value:,.2f}")
        print(f"绝对收益: {Fore.GREEN if absolute_return >= 0 else Fore.RED}${absolute_return:,.2f}{Style.RESET_ALL}")
        print(f"总收益率: {Fore.GREEN if total_return >= 0 else Fore.RED}{total_return:.2f}%{Style.RESET_ALL}")
        
        print(f"\n最终持仓:")
        print(f"现金: ${self.portfolio['cash']:,.2f}")
        
        total_position_value = 0
        for ticker in self.tickers:
            pos = self.portfolio["positions"][ticker]
            if pos["long"] > 0:
                value = pos["long"] * end_prices[ticker]
                total_position_value += value
                price_change = ((end_prices[ticker] - start_prices[ticker]) / start_prices[ticker]) * 100
                print(f"{ticker}: {pos['long']} 股 @ ${end_prices[ticker]:.2f} (价值: ${value:,.2f}, 股价变化: {price_change:+.2f}%)")

        print(f"总股票价值: ${total_position_value:,.2f}")

        # 计算基准收益（等权重买入持有）
        benchmark_return = 0
        for ticker in self.tickers:
            ticker_return = ((end_prices[ticker] - start_prices[ticker]) / start_prices[ticker]) * 100
            benchmark_return += ticker_return
        benchmark_return /= len(self.tickers)

        print(f"\n基准收益 (等权重): {Fore.CYAN}{benchmark_return:.2f}%{Style.RESET_ALL}")
        print(f"相对基准超额收益: {Fore.GREEN if (total_return - benchmark_return) >= 0 else Fore.RED}{total_return - benchmark_return:.2f}%{Style.RESET_ALL}")

        return {
            "initial_capital": self.initial_capital,
            "final_value": final_portfolio_value,
            "total_return": total_return,
            "absolute_return": absolute_return,
            "benchmark_return": benchmark_return,
            "excess_return": total_return - benchmark_return,
            "start_prices": start_prices,
            "end_prices": end_prices,
            "decisions": decisions,
            "analyst_signals": analyst_signals
        }

    def analyze_performance(self):
        """创建性能DataFrame并绘制权益曲线"""
        if not self.portfolio_values:
            print("未找到投资组合数据。请先运行回测。")
            return pd.DataFrame()

        performance_df = pd.DataFrame(self.portfolio_values).set_index("Date")
        if performance_df.empty:
            print("没有有效的性能数据可供分析。")
            return performance_df

        # 绘制投资组合价值随时间变化的图表
        plt.figure(figsize=(12, 6))
        plt.plot(performance_df.index, performance_df["Portfolio Value"], color="blue", linewidth=2, marker='o', markersize=8)
        plt.title("Buy and Hold 策略 - 投资组合价值", fontsize=14, fontweight='bold')
        plt.ylabel("投资组合价值 ($)")
        plt.xlabel("日期")
        plt.grid(True, alpha=0.3)
        
        # 格式化Y轴为货币格式
        ax = plt.gca()
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        plt.tight_layout()
        plt.show()

        return performance_df


### 运行 Auto Backtester
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="运行自动回测模拟 (Buy and Hold 策略)")
    parser.add_argument(
        "--tickers",
        type=str,
        required=True,
        help="逗号分隔的股票代码列表 (例如: AAPL,MSFT,GOOGL)",
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
        help="初始资金 (默认: 100000)",
    )
    parser.add_argument(
        "--margin-requirement",
        type=float,
        default=0.0,
        help="做空头寸的保证金比例，例如 0.5 表示 50% (默认: 0.0)",
    )

    args = parser.parse_args()

    # 解析股票代码
    tickers = [ticker.strip().upper() for ticker in args.tickers.split(",")]
    
    print(f"{Fore.CYAN}自动回测器 - Buy and Hold 策略{Style.RESET_ALL}")
    print(f"股票代码: {', '.join(tickers)}")

    # 自动选择指定的分析师和模型
    selected_analysts = ["ben_graham", "charlie_munger", "warren_buffett"]
    model_name = "gpt-4o-mini"
    model_provider = "OpenAI"

    print(f"自动选择的分析师: {', '.join([name.replace('_', ' ').title() for name in selected_analysts])}")
    print(f"自动选择的模型: {model_name} ({model_provider})")

    # 创建并运行自动回测器
    auto_backtester = AutoBacktester(
        agent=run_hedge_fund,
        tickers=tickers,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_capital=args.initial_capital,
        model_name=model_name,
        model_provider=model_provider,
        selected_analysts=selected_analysts,
        initial_margin_requirement=args.margin_requirement,
    )

    performance_metrics = auto_backtester.run_backtest()
    performance_df = auto_backtester.analyze_performance() 