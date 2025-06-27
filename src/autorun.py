import sys
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from colorama import Fore, Style, init
import json

# 导入所需模块
from src.main import run_hedge_fund, create_workflow
from src.utils.display import print_trading_output
from src.llm.models import ModelProvider

# 加载环境变量
load_dotenv()
init(autoreset=True)


def save_result_to_file(ticker: str, result: dict, filename: str = "result.txt"):
    """将结果追加保存到文件中（不覆盖之前的内容）"""
    with open(filename, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"股票分析报告: {ticker}\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*60}\n\n")
        
        decisions = result.get("decisions")
        if not decisions:
            f.write("没有可用的交易决策\n")
            return
            
        # 获取该股票的决策
        decision = decisions.get(ticker, {})
        
        # 写入分析师信号
        f.write("分析师信号:\n")
        f.write("-" * 40 + "\n")
        
        for agent, signals in result.get("analyst_signals", {}).items():
            if ticker not in signals:
                continue
                
            # 跳过风险管理代理
            if agent == "risk_management_agent":
                continue
                
            signal = signals[ticker]
            agent_name = agent.replace("_agent", "").replace("_", " ").title()
            signal_type = signal.get("signal", "").upper()
            confidence = signal.get("confidence", 0)
            reasoning = signal.get("reasoning", "")
            
            f.write(f"分析师: {agent_name}\n")
            f.write(f"信号: {signal_type}\n")
            f.write(f"信心度: {confidence}%\n")
            f.write(f"理由: {reasoning}\n")
            f.write("-" * 40 + "\n")
        
        # 写入交易决策
        f.write("\n交易决策:\n")
        f.write("-" * 40 + "\n")
        f.write(f"操作: {decision.get('action', '').upper()}\n")
        f.write(f"数量: {decision.get('quantity', 0)}\n")
        f.write(f"信心度: {decision.get('confidence', 0):.1f}%\n")
        f.write(f"理由: {decision.get('reasoning', '')}\n")
        f.write("\n")


def run_single_ticker(
    ticker: str,
    start_date: str,
    end_date: str,
    portfolio: dict,
    show_reasoning: bool = False,
):
    """运行单只股票的分析"""
    print(f"\n{Fore.CYAN}{Style.BRIGHT}正在分析股票: {ticker}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}时间范围: {start_date} 到 {end_date}{Style.RESET_ALL}")
    
    # 自动选择的分析师 (Ben Graham, Charlie Munger, Warren Buffett)
    selected_analysts = ["ben_graham", "charlie_munger", "warren_buffett"]
    
    # 自动选择的模型 (OpenAI o4 mini)
    model_name = "o4-mini"
    model_provider = ModelProvider.OPENAI.value
    
    print(f"{Fore.GREEN}已选择分析师: Ben Graham, Charlie Munger, Warren Buffett{Style.RESET_ALL}")
    print(f"{Fore.GREEN}已选择模型: OpenAI o4 mini{Style.RESET_ALL}\n")
    
    try:
        # 运行对冲基金分析
        result = run_hedge_fund(
            tickers=[ticker],  # 单只股票
            start_date=start_date,
            end_date=end_date,
            portfolio=portfolio,
            show_reasoning=show_reasoning,
            selected_analysts=selected_analysts,
            model_name=model_name,
            model_provider=model_provider,
        )
        
        # 显示结果
        print_trading_output(result)
        
        # 保存结果到文件
        save_result_to_file(ticker, result)
        print(f"\n{Fore.GREEN}结果已保存到 result.txt{Style.RESET_ALL}")
        
        return result
        
    except Exception as e:
        print(f"{Fore.RED}分析 {ticker} 时出错: {str(e)}{Style.RESET_ALL}")
        return None


def main():
    parser = argparse.ArgumentParser(description="自动运行对冲基金交易系统")
    parser.add_argument("--initial-cash", type=float, default=100000.0, help="初始现金头寸 (默认: 100000.0)")
    parser.add_argument("--margin-requirement", type=float, default=0.0, help="初始保证金要求 (默认: 0.0)")
    parser.add_argument("--tickers", type=str, required=True, help="股票代码列表，用逗号分隔")
    parser.add_argument(
        "--start-date",
        type=str,
        help="开始日期 (YYYY-MM-DD). 默认为结束日期前3个月",
    )
    parser.add_argument("--end-date", type=str, help="结束日期 (YYYY-MM-DD). 默认为今天")
    parser.add_argument("--show-reasoning", action="store_true", help="显示各代理的推理过程")
    
    args = parser.parse_args()
    
    # 解析股票代码
    tickers = [ticker.strip().upper() for ticker in args.tickers.split(",")]
    
    # 验证日期格式
    if args.start_date:
        try:
            datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("开始日期必须为 YYYY-MM-DD 格式")

    if args.end_date:
        try:
            datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("结束日期必须为 YYYY-MM-DD 格式")
    
    # 设置开始和结束日期
    end_date = args.end_date or datetime.now().strftime("%Y-%m-%d")
    if not args.start_date:
        # 计算结束日期前3个月
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = (end_date_obj - relativedelta(months=3)).strftime("%Y-%m-%d")
    else:
        start_date = args.start_date
    
    # 追加运行记录到结果文件（不覆盖之前的结果）
    with open("result.txt", "a", encoding="utf-8") as f:
        f.write(f"\n\n{'='*80}\n")
        f.write(f"新的分析会话开始\n")
        f.write(f"{'='*80}\n")
        f.write(f"对冲基金交易系统分析报告\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"分析期间: {start_date} 到 {end_date}\n")
        f.write(f"股票列表: {', '.join(tickers)}\n")
        f.write(f"选择的分析师: Ben Graham, Charlie Munger, Warren Buffett\n")
        f.write(f"使用的模型: OpenAI o4 mini\n")
        f.write(f"{'='*80}\n")
    
    print(f"{Fore.CYAN}{Style.BRIGHT}开始自动分析{Style.RESET_ALL}")
    print(f"{Fore.WHITE}股票数量: {len(tickers)}{Style.RESET_ALL}")
    print(f"{Fore.WHITE}分析期间: {start_date} 到 {end_date}{Style.RESET_ALL}")
    
    # 逐个分析每只股票
    for i, ticker in enumerate(tickers, 1):
        print(f"\n{Fore.BLUE}{Style.BRIGHT}=== 处理第 {i}/{len(tickers)} 只股票 ==={Style.RESET_ALL}")
        
        # 为每只股票创建独立的投资组合
        portfolio = {
            "cash": args.initial_cash,
            "margin_requirement": args.margin_requirement,
            "margin_used": 0.0,
            "positions": {
                ticker: {
                    "long": 0,
                    "short": 0,
                    "long_cost_basis": 0.0,
                    "short_cost_basis": 0.0,
                    "short_margin_used": 0.0,
                }
            },
            "realized_gains": {
                ticker: {
                    "long": 0.0,
                    "short": 0.0,
                }
            },
        }
        
        # 运行分析
        result = run_single_ticker(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            portfolio=portfolio,
            show_reasoning=args.show_reasoning,
        )
        
        if result:
            print(f"{Fore.GREEN}✓ {ticker} 分析完成{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}✗ {ticker} 分析失败{Style.RESET_ALL}")
    
    print(f"\n{Fore.GREEN}{Style.BRIGHT}所有股票分析完成！{Style.RESET_ALL}")
    print(f"{Fore.WHITE}详细结果已保存到 result.txt 文件中{Style.RESET_ALL}")


if __name__ == "__main__":
    main()