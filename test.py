import os
import requests
from dotenv import load_dotenv

# 加载 .env 文件中的环境变量
load_dotenv(
    dotenv_path=".env",  # 指定 .env 文件路径
    override=True,       # 强制覆盖已存在的环境变量
    verbose=True        # 显示调试信息
)# 从环境变量中获取配置
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE")

# 测试 API 连接
test_url = f"{OPENAI_API_BASE}/chat/completions"
headers = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json"
}
data = {
    "model": "gpt-4o",
    "messages": [{"role": "user", "content": "Hello"}]
}

# 打印配置信息（调试用）
print(f"API Base URL: {OPENAI_API_BASE}")
print(f"Test URL: {test_url}")

response = requests.post(test_url, headers=headers, json=data)
print(f"Status Code: {response.status_code}")
print(f"Response: {response.json()}")