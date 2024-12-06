from workflow_manager import WorkflowManager
from dotenv import load_dotenv
import os

# 加载环境变量
load_dotenv()

# 初始化工作流管理器
manager = WorkflowManager()  # 将从环境变量读取配置

