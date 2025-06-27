import os
import requests
import json
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

def get_etf_holdings(symbol="RUT"):
    """
    获取ETF的成分股信息
    
    Args:
        symbol (str): ETF符号，默认为RUT
    
    Returns:
        list: 成分股符号列表
    """
    
    # 从环境变量获取API密钥
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    
    if not api_key:
        print("错误：未找到ALPHAVANTAGE_API_KEY环境变量")
        print("请在.env文件中设置您的Alpha Vantage API密钥")
        return None
    
    # 构建API请求URL
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "ETF_PROFILE",
        "symbol": symbol,
        "apikey": api_key
    }
    
    try:
        print(f"正在获取 {symbol} 的成分股信息...")
        print(f"请求URL: {url}")
        print(f"请求参数: {params}")
        
        # 发送请求
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        print(f"HTTP状态码: {response.status_code}")
        
        # 解析JSON响应
        data = response.json()
        
        # 检查是否有错误信息
        if "Error Message" in data:
            print(f"API错误：{data['Error Message']}")
            return None
        
        if "Note" in data:
            print(f"API限制：{data['Note']}")
            return None
        
        # 检查响应是否为空
        if not data:
            print("警告：API返回空响应")
            return None
        
        # 提取成分股信息
        holdings = []
        if "holdings" in data:
            for holding in data["holdings"]:
                if "symbol" in holding:
                    symbol = holding["symbol"]
                    # 过滤掉 n/a 和空值
                    if symbol and symbol.lower() != "n/a" and symbol.strip():
                        holdings.append(symbol)
        
        # 如果没有找到holdings字段，尝试查看完整响应结构
        if not holdings:
            print("响应数据结构：")
            print(json.dumps(data, indent=2))
            return None
        
        return holdings
        
    except requests.exceptions.RequestException as e:
        print(f"网络请求错误：{e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON解析错误：{e}")
        return None
    except Exception as e:
        print(f"未知错误：{e}")
        return None

def main():
    """主函数"""
    # RUT是指数，不是ETF。Russell 2000的ETF通常是IWM
    symbol = "IWM"  # iShares Russell 2000 ETF
    
    print(f"注意：RUT是Russell 2000指数，我们将获取其对应的ETF {symbol} 的成分股")
    
    # 获取成分股
    holdings = get_etf_holdings(symbol)
    
    if holdings:
        print(f"\n{symbol} ETF 成分股总数：{len(holdings)}")
        
        # 按每100只分组
        group_size = 100
        groups = [holdings[i:i + group_size] for i in range(0, len(holdings), group_size)]
        
        print(f"\n分组显示（每组{group_size}只，共{len(groups)}组）：")
        print("=" * 80)
        
        # 分组显示
        for i, group in enumerate(groups, 1):
            print(f"\n第{i}组（{len(group)}只）：")
            group_str = ",".join(group)
            print(group_str)
        
        print("\n" + "=" * 80)
        
        # 完整列表（以逗号分隔的格式）
        all_holdings_str = ",".join(holdings)
        print(f"\n完整成分股列表：")
        print(all_holdings_str)
        
        # 保存到文件
        with open(f"{symbol}_holdings.txt", "w", encoding="utf-8") as f:
            f.write("# 分组显示\n")
            for i, group in enumerate(groups, 1):
                f.write(f"\n# 第{i}组（{len(group)}只）：\n")
                f.write(",".join(group) + "\n")
            
            f.write(f"\n# 完整列表：\n")
            f.write(all_holdings_str)
        
        print(f"\n成分股列表已保存到 {symbol}_holdings.txt 文件中")
    else:
        print("获取成分股失败")

if __name__ == "__main__":
    main()

