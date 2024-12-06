manager = WorkflowManager(
    uri="neo4j://localhost:7687",
    user="neo4j",
    password="password"
)

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