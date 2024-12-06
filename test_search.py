from workflow_manager import WorkflowManager
import json
from dotenv import load_dotenv
import os

# 加载环境变量
load_dotenv()

def print_results(results):
    """格式化打印搜索结果"""
    for item in results.items:
        # 解析Record字符串为字典
        record_str = item.content.replace("<Record ", "").replace(">", "")
        parts = [p.split('=') for p in record_str.split(' ') if '=' in p]
        record = {}
        for key, value in parts:
            try:
                record[key] = eval(value)
            except:
                record[key] = value.strip("'")
        
        # 打印结果
        print("\n相似度：{:.2f}".format(record['similarity_score']))
        
        if 'workflow_name' in record:  # 工作流搜索结果
            print(f"工作流：{record['workflow_name']}")
            print(f"描述：{record['workflow_description']}")
            print("\n包含任务：")
            for task in record['tasks']:
                print(f"- {task['name']} (顺序: {task['order']}, 工具: {task['tool']})")
                print(f"  描述: {task.get('description', '无描述')}")
        
        elif 'task_name' in record:  # 任务搜索结果
            print(f"任务：{record['task_name']}")
            print(f"描述：{record['task_description']}")
            print(f"使用工具：{record['tool_name']}")
            if record['workflows']:
                print("\n所属工作流：")
                for wf in record['workflows']:
                    print(f"- {wf['name']} (顺序: {wf['order']})")
        
        elif 'tool_name' in record:  # 工具搜索结果
            print(f"工具：{record['tool_name']}")
            print(f"描述：{record['tool_description']}")
            print(f"函数：{record['tool_function']}")
            if record['used_by_tasks']:
                print("\n被以下任务使用：")
                for task in record['used_by_tasks']:
                    print(f"- {task}")
        
        print("\n" + "-"*50)

# 初始化工作流管理器
manager = WorkflowManager(
    uri=os.getenv("NEO4J_URI", "neo4j://localhost:7687"),
    user=os.getenv("NEO4J_USER", "neo4j"),
    password=os.getenv("NEO4J_PASSWORD", "password")
)

# 创建一些测试工具
tools = [
    {
        "name": "data_preprocessor",
        "description": "数据预处理工具，用于清洗和准备数据",
        "function": "lambda x: {'output': x['input']}"
    },
    {
        "name": "feature_engineer",
        "description": "特征工程工具，用于特征提取和转换",
        "function": "lambda x: {'features': x['output']}"
    },
    {
        "name": "model_trainer",
        "description": "模型训练工具，用于训练机器学习模型",
        "function": "lambda x: {'model': 'trained_model'}"
    },
    {
        "name": "data_validator",
        "description": "数据验证工具，用于检查数据质量和完整性",
        "function": "lambda x: {'is_valid': True}"
    }
]

# 创建工具
for tool in tools:
    manager.create_tool(**tool)

# 创建一些测试任务
tasks = [
    {
        "name": "数据预处理",
        "description": "清洗和准备数据，确保数据质量",
        "tool_name": "data_preprocessor"
    },
    {
        "name": "特征工程",
        "description": "进行特征提取和转换",
        "tool_name": "feature_engineer"
    },
    {
        "name": "模型训练",
        "description": "训练机器学习模型",
        "tool_name": "model_trainer"
    },
    {
        "name": "数据验证",
        "description": "验证数据质量和完整性",
        "tool_name": "data_validator"
    }
]

# 创建任务
for task in tasks:
    manager.create_task(**task)

# 创建一些测试工作流
workflows = [
    {
        "name": "数据分析流程",
        "description": "完整的数据分析和模型训练流程",
        "tasks": [
            {"name": "数据预处理", "order": 1},
            {"name": "特征工程", "order": 2},
            {"name": "模型训练", "order": 3}
        ]
    },
    {
        "name": "数据质量检查流程",
        "description": "数据质量验证和预处理流程",
        "tasks": [
            {"name": "数据验证", "order": 1},
            {"name": "数据预处理", "order": 2}
        ]
    }
]

# 创建工作流
for workflow in workflows:
    manager.create_workflow(**workflow)

print("\n=== 测试工作流搜索 ===")
print("\n1. 搜索包含'数据分析'的工作流:")
results = manager.search_workflows("数据分析")
print_results(results)

print("\n2. 搜索包含'质量检查'的工作流:")
results = manager.search_workflows("质量检查")
print_results(results)

print("\n=== 测试任务搜索 ===")
print("\n1. 搜索包含'预处理'的任务:")
results = manager.search_tasks("预处理")
print_results(results)

print("\n2. 搜索包含'特征'的任务:")
results = manager.search_tasks("特征")
print_results(results)

print("\n=== 测试工具搜索 ===")
print("\n1. 搜索包含'数据'的工具:")
results = manager.search_tools("数据")
print_results(results)

print("\n2. 搜索包含'模型'的工具:")
results = manager.search_tools("模型")
print_results(results)

# 清理连接
manager.close()
