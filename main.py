from workflow_manager import WorkflowManager
from dotenv import load_dotenv
import os

# 加载环境变量
load_dotenv()

# 初始化工作流管理器
manager = WorkflowManager()  # 将从环境变量读取配置

# 创建任务
task = manager.create_task(
    name="数据预处理",
    description="清洗和准备数据",
    tool_name="data_preprocessor"
)

# 创建工作流
workflow = manager.create_workflow(
    name="数据分析流程",
    description="完整的数据分析流程",
    tasks=[
        {"name": "数据预处理", "order": 1},
        {"name": "特征工程", "order": 2},
        {"name": "模型训练", "order": 3}
    ]
)

# 执行工作流
result = manager.execute_workflow(
    workflow_name="数据分析流程",
    context_variables={"input_data": "path/to/data.csv"}
)