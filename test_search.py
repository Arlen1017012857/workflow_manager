from workflow_manager import WorkflowManager
import json
from dotenv import load_dotenv
import os

# 加载环境变量
load_dotenv()

def print_results(results, result_type="workflow"):
    """格式化打印搜索结果
    
    Args:
        results: 解析后的搜索结果列表
        result_type: 结果类型，可选值: "workflow", "task", "tool"
    """
    print("\n=== Search Results ===")
    
    for item in results:
        print(f"\nName: {item['name']}")
        print(f"Description: {item['description']}")
        print(f"Similarity Score: {item['similarity_score']:.2f}")
        
        if result_type == "workflow":
            print("\nTasks:")
            for task in item['tasks']:
                print(f"  {task['order']}. {task['name']}")
                print(f"     Tool: {task['tool']}")
                print(f"     Description: {task['description']}")
                
        elif result_type == "task":
            print(f"Tool: {item['tool']}")
            print("\nUsed in Workflows:")
            for workflow in item['workflows']:
                print(f"  - {workflow['name']} (Order: {workflow['order']})")
                
        elif result_type == "tool":
            print(item)
            print(f"tool_code: {item['tool_code']}")
            print("\nUsed by Tasks:")
            for task in item['used_by_tasks']:
                print(f"  - {task}")
                
        print("-" * 50)


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
        "tool_code": "lambda x: {'output': x['input']}"
    },
    {
        "name": "feature_engineer",
        "description": "特征工程工具，用于特征提取和转换",
        "tool_code": "lambda x: {'features': x['output']}"
    },
    {
        "name": "model_trainer",
        "description": "模型训练工具，用于训练机器学习模型",
        "tool_code": "lambda x: {'model': 'trained_model'}"
    },
    {
        "name": "data_validator",
        "description": "数据验证工具，用于检查数据质量和完整性",
        "tool_code": "lambda x: {'is_valid': True}"
    }
]

# 创建工具
# for tool in tools:
#     manager.create_tool(**tool)

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

# # 创建任务
# for task in tasks:
#     manager.create_task(**task)

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
# for workflow in workflows:
#     manager.create_workflow(**workflow)

# print("\n=== 测试工作流搜索 ===")
# print("\n1. 搜索包含'数据分析'的工作流:")
# results = manager.search_workflows("数据分析")
# print_results(results, result_type="workflow")

# print("\n2. 搜索包含'质量检查'的工作流:")
# results = manager.search_workflows("质量检查")
# print_results(results, result_type="workflow")

print("\n=== 测试任务搜索 ===")
print("\n1. 搜索包含'预处理'的任务:")
results = manager.search_tasks("预处理")
print_results(results, result_type="task")

# print("\n2. 搜索包含'特征'的任务:")
# results = manager.search_tasks("特征")
# print_results(results, result_type="task")

print("\n=== 测试工具搜索 ===")
print("\n1. 搜索包含'数据'的工具:")
results = manager.search_tools("数据")
print_results(results, result_type="tool")

# print("\n2. 搜索包含'模型'的工具:")
# results = manager.search_tools("模型")
# print_results(results, result_type="tool")

# 清理连接
# manager.close()
