import sys

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import argparse

import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
from colorama import Fore, Style, init
import numpy as np

# 设置matplotlib支持中文
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

from src.tools.api import (
    get_price_data,
    get_prices,
)

init(autoreset=True)


class EqualWeightBacktester:
    def __init__(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
        initial_capital: float,
    ):
        """
        等权重买入持有策略回测器
        
        :param tickers: 要回测的股票代码列表.
        :param start_date: 开始日期字符串 (YYYY-MM-DD).
        :param end_date: 结束日期字符串 (YYYY-MM-DD).
        :param initial_capital: 初始资金.
        """
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital

        # 初始化投资组合
        self.portfolio_values = []
        self.portfolio = {
            "cash": 0.0,  # 全仓投资后剩余现金
            "positions": {ticker: {"shares": 0, "cost_basis": 0.0} for ticker in tickers},
        }

    def execute_equal_weight_purchase(self, prices):
        """
        执行等权重买入
        """
        capital_per_stock = self.initial_capital / len(self.tickers)
        total_invested = 0
        
        print(f"\n执行等权重买入 ({self.start_date}):")
        print(f"每只股票分配资金: ${capital_per_stock:,.2f}")
        
        for ticker in self.tickers:
            price = prices[ticker]
            shares = int(capital_per_stock / price)  # 买入整数股
            actual_cost = shares * price
            
            self.portfolio["positions"][ticker]["shares"] = shares
            self.portfolio["positions"][ticker]["cost_basis"] = price
            total_invested += actual_cost
            
            print(f"  {ticker}: 买入 {shares} 股 @ ${price:.2f} (投入: ${actual_cost:,.2f})")
        
        self.portfolio["cash"] = self.initial_capital - total_invested
        print(f"总投入: ${total_invested:,.2f}")
        print(f"剩余现金: ${self.portfolio['cash']:,.2f}")

    def calculate_portfolio_value(self, current_prices):
        """
        计算投资组合总价值
        """
        total_value = self.portfolio["cash"]

        for ticker in self.tickers:
            position = self.portfolio["positions"][ticker]
            price = current_prices[ticker]
            position_value = position["shares"] * price
            total_value += position_value

        return total_value

    def prefetch_data(self):
        """预取回测期间所需的价格数据"""
        print("\n预取价格数据...")

        # 获取稍微宽一点的日期范围确保有数据
        start_date_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        
        # 扩展日期范围以确保获得数据
        extended_start = (start_date_dt - timedelta(days=10)).strftime("%Y-%m-%d")
        extended_end = (end_date_dt + timedelta(days=10)).strftime("%Y-%m-%d")

        for ticker in self.tickers:
            # 获取价格数据
            get_prices(ticker, extended_start, extended_end)

        print("数据预取完成。")

    def run_backtest(self):
        # 预取所有数据
        self.prefetch_data()

        print(f"\n{Fore.CYAN}等权重买入持有策略回测{Style.RESET_ALL}")
        print(f"股票: {', '.join(self.tickers)}")
        print(f"开始日期: {self.start_date}")
        print(f"结束日期: {self.end_date}")
        print(f"初始资金: ${self.initial_capital:,.2f}")

        # ---------------------------------------------------------------
        # 1) 在开始日期获取价格并执行等权重买入
        # ---------------------------------------------------------------
        try:
            start_prices = {}
            missing_data = False

            for ticker in self.tickers:
                try:
                    # 获取开始日期附近的价格数据
                    start_date_range_start = (datetime.strptime(self.start_date, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
                    price_data = get_price_data(ticker, start_date_range_start, self.start_date)
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

        # 执行等权重买入
        self.execute_equal_weight_purchase(start_prices)

        # 记录初始投资后的投资组合状态
        initial_portfolio_value = self.calculate_portfolio_value(start_prices)
        self.portfolio_values.append({
            "Date": datetime.strptime(self.start_date, "%Y-%m-%d"),
            "Portfolio Value": initial_portfolio_value
        })

        print(f"\n初始投资后的投资组合价值: ${initial_portfolio_value:,.2f}")

        # ---------------------------------------------------------------
        # 2) 计算整个投资期间每个交易日的投资组合价值
        # ---------------------------------------------------------------
        print(f"\n计算投资期间每日投资组合价值...")
        
        # 生成交易日期范围
        dates = pd.date_range(self.start_date, self.end_date, freq="B")  # B = 工作日
        
        for current_date in dates[1:]:  # 跳过第一个日期（已经记录）
            current_date_str = current_date.strftime("%Y-%m-%d")
            
            try:
                current_prices = {}
                missing_data = False

                for ticker in self.tickers:
                    try:
                        # 获取当前日期附近的价格数据
                        date_range_start = (current_date - timedelta(days=5)).strftime("%Y-%m-%d")
                        price_data = get_price_data(ticker, date_range_start, current_date_str)
                        if price_data.empty:
                            # 如果当天没有数据，使用前一个有效价格
                            continue
                        current_prices[ticker] = price_data.iloc[-1]["close"]
                    except Exception as e:
                        # 如果获取价格失败，跳过这个股票
                        continue

                # 如果我们有足够的价格数据，计算投资组合价值
                if len(current_prices) == len(self.tickers):
                    daily_portfolio_value = self.calculate_portfolio_value(current_prices)
                    self.portfolio_values.append({
                        "Date": current_date,
                        "Portfolio Value": daily_portfolio_value
                    })
                elif len(current_prices) > 0:
                    # 如果只有部分股票有价格数据，使用可用的价格和前一日的价格
                    # 获取前一日的价格作为备用
                    prev_date = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
                    for ticker in self.tickers:
                        if ticker not in current_prices:
                            try:
                                prev_range_start = (current_date - timedelta(days=10)).strftime("%Y-%m-%d")
                                prev_price_data = get_price_data(ticker, prev_range_start, prev_date)
                                if not prev_price_data.empty:
                                    current_prices[ticker] = prev_price_data.iloc[-1]["close"]
                            except:
                                # 如果还是没有数据，使用开始日期的价格
                                current_prices[ticker] = start_prices.get(ticker, 0)
                    
                    if len(current_prices) == len(self.tickers):
                        daily_portfolio_value = self.calculate_portfolio_value(current_prices)
                        self.portfolio_values.append({
                            "Date": current_date,
                            "Portfolio Value": daily_portfolio_value
                        })

            except Exception as e:
                # 如果当天出错，跳过
                continue

        # 确保我们有结束日期的数据
        try:
            end_prices = {}
            missing_data = False

            for ticker in self.tickers:
                try:
                    end_date_range_start = (datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
                    price_data = get_price_data(ticker, end_date_range_start, self.end_date)
                    if price_data.empty:
                        print(f"警告: {ticker} 在 {self.end_date} 没有价格数据")
                        # 使用最后可用的价格
                        if self.portfolio_values:
                            # 使用最近一次计算的价格
                            end_prices[ticker] = start_prices[ticker]  # 备用方案
                        else:
                            missing_data = True
                            break
                    else:
                        end_prices[ticker] = price_data.iloc[-1]["close"]
                except Exception as e:
                    print(f"获取 {ticker} 在 {self.end_date} 的价格时出错: {e}")
                    end_prices[ticker] = start_prices[ticker]  # 使用开始价格作为备用

            # 计算最终投资组合价值
            if not missing_data:
                final_portfolio_value = self.calculate_portfolio_value(end_prices)
                
                # 检查是否已经有结束日期的记录
                end_date_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
                has_end_date = any(record["Date"] == end_date_dt for record in self.portfolio_values)
                
                if not has_end_date:
                    self.portfolio_values.append({
                        "Date": end_date_dt,
                        "Portfolio Value": final_portfolio_value
                    })
            else:
                # 如果缺少数据，使用最后一个可用的投资组合价值
                if self.portfolio_values:
                    final_portfolio_value = self.portfolio_values[-1]["Portfolio Value"]
                    end_prices = start_prices  # 使用开始价格作为备用
                else:
                    return {}

        except Exception as e:
            print(f"获取 {self.end_date} 价格时出错: {e}")
            if self.portfolio_values:
                final_portfolio_value = self.portfolio_values[-1]["Portfolio Value"]
                end_prices = start_prices
            else:
                return {}
        
        print(f"成功计算了 {len(self.portfolio_values)} 个交易日的投资组合价值")

        # 计算收益
        total_return = ((final_portfolio_value - self.initial_capital) / self.initial_capital) * 100
        absolute_return = final_portfolio_value - self.initial_capital

        print(f"\n{Fore.WHITE}{Style.BRIGHT}等权重投资组合最终结果:{Style.RESET_ALL}")
        print(f"初始资金: ${self.initial_capital:,.2f}")
        print(f"最终价值: ${final_portfolio_value:,.2f}")
        print(f"绝对收益: {Fore.GREEN if absolute_return >= 0 else Fore.RED}${absolute_return:,.2f}{Style.RESET_ALL}")
        print(f"总收益率: {Fore.GREEN if total_return >= 0 else Fore.RED}{total_return:.2f}%{Style.RESET_ALL}")
        
        print(f"\n最终持仓详情:")
        print(f"现金: ${self.portfolio['cash']:,.2f}")
        
        total_position_value = 0
        individual_returns = []
        
        for ticker in self.tickers:
            position = self.portfolio["positions"][ticker]
            if position["shares"] > 0:
                current_value = position["shares"] * end_prices[ticker]
                cost_basis = position["shares"] * position["cost_basis"]
                individual_return = ((current_value - cost_basis) / cost_basis) * 100
                individual_returns.append(individual_return)
                
                total_position_value += current_value
                price_change = ((end_prices[ticker] - start_prices[ticker]) / start_prices[ticker]) * 100
                
                print(f"{ticker}: {position['shares']} 股")
                print(f"  买入价: ${position['cost_basis']:.2f} -> 当前价: ${end_prices[ticker]:.2f}")
                print(f"  投入: ${cost_basis:,.2f} -> 当前价值: ${current_value:,.2f}")
                print(f"  股价变化: {Fore.GREEN if price_change >= 0 else Fore.RED}{price_change:+.2f}%{Style.RESET_ALL}")
                print(f"  个股收益: {Fore.GREEN if individual_return >= 0 else Fore.RED}{individual_return:+.2f}%{Style.RESET_ALL}")
                print()

        print(f"总股票价值: ${total_position_value:,.2f}")
        
        # 计算统计信息
        if individual_returns:
            avg_individual_return = sum(individual_returns) / len(individual_returns)
            best_stock = max(individual_returns)
            worst_stock = min(individual_returns)
            
            print(f"\n{Fore.YELLOW}投资统计:{Style.RESET_ALL}")
            print(f"平均个股收益: {Fore.CYAN}{avg_individual_return:.2f}%{Style.RESET_ALL}")
            print(f"最佳个股收益: {Fore.GREEN}{best_stock:.2f}%{Style.RESET_ALL}")
            print(f"最差个股收益: {Fore.RED}{worst_stock:.2f}%{Style.RESET_ALL}")
            print(f"收益标准差: {Fore.CYAN}{np.std(individual_returns):.2f}%{Style.RESET_ALL}")

        return {
            "initial_capital": self.initial_capital,
            "final_value": final_portfolio_value,
            "total_return": total_return,
            "absolute_return": absolute_return,
            "start_prices": start_prices,
            "end_prices": end_prices,
            "individual_returns": individual_returns
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
        plt.plot(performance_df.index, performance_df["Portfolio Value"], 
                color="blue", linewidth=3, marker='o', markersize=6)
        plt.title("等权重买入持有策略 - 投资组合价值", fontsize=16, fontweight='bold')
        plt.ylabel("投资组合价值 ($)", fontsize=12)
        plt.xlabel("日期", fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # 格式化Y轴为货币格式
        ax = plt.gca()
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # 只标注关键的四个值：起始值、最高值、最低值、终值
        values = performance_df["Portfolio Value"]
        
        # 找到关键点
        start_date = values.index[0]
        end_date = values.index[-1]
        max_value_date = values.idxmax()
        min_value_date = values.idxmin()
        
        start_value = values.iloc[0]
        end_value = values.iloc[-1]
        max_value = values.max()
        min_value = values.min()
        
        # 标注起始值
        plt.annotate(f'起始: ${start_value:,.0f}', 
                    (start_date, start_value), 
                    textcoords="offset points", 
                    xytext=(-20, 15), 
                    ha='center',
                    fontsize=10,
                    fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.7))
        
        # 标注最高值
        plt.annotate(f'最高: ${max_value:,.0f}', 
                    (max_value_date, max_value), 
                    textcoords="offset points", 
                    xytext=(0, 20), 
                    ha='center',
                    fontsize=10,
                    fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.7))
        
        # 标注最低值
        plt.annotate(f'最低: ${min_value:,.0f}', 
                    (min_value_date, min_value), 
                    textcoords="offset points", 
                    xytext=(0, -25), 
                    ha='center',
                    fontsize=10,
                    fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightcoral", alpha=0.7))
        
        # 标注终值
        plt.annotate(f'终值: ${end_value:,.0f}', 
                    (end_date, end_value), 
                    textcoords="offset points", 
                    xytext=(20, 15), 
                    ha='center',
                    fontsize=10,
                    fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", alpha=0.7))
        
        plt.tight_layout()
        plt.show()

        return performance_df


### 运行等权重回测器
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="运行等权重买入持有策略回测")
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

    args = parser.parse_args()

    # 解析股票代码
    tickers = [ticker.strip().upper() for ticker in args.tickers.split(",")]
    
    print(f"{Fore.CYAN}等权重买入持有策略回测器{Style.RESET_ALL}")
    print(f"股票代码: {', '.join(tickers)}")

    # 创建并运行等权重回测器
    equal_weight_backtester = EqualWeightBacktester(
        tickers=tickers,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_capital=args.initial_capital,
    )

    performance_metrics = equal_weight_backtester.run_backtest()
    performance_df = equal_weight_backtester.analyze_performance()
