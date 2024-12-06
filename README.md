# Workflow Manager

基于Neo4j图数据库的工作流管理系统，支持工作流、任务和工具的管理。

## 特点

- 使用图数据库存储和管理工作流、任务和工具之间的关系
- 支持向量检索和全文检索
- 灵活的任务顺序管理
- 上下文变量在任务间传递
- 支持工具函数的动态执行

## 安装

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 设置环境变量：
```bash
EMBEDDER_BASE_URL=your_embedder_base_url
EMBEDDER_API_KEY=your_api_key
EMBEDDER_MODEL=your_model_name
```

## 使用示例

```python
from workflow_manager import WorkflowManager

# 初始化工作流管理器
manager = WorkflowManager(
    uri="neo4j://localhost:7687",
    user="neo4j",
    password="password"
)

# 创建任务
manager.create_task(
    name="数据预处理",
    description="清洗和准备数据",
    tool_name="data_preprocessor"
)

# 创建工作流
manager.create_workflow(
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

# 搜索工作流
workflows = manager.search_workflows("数据分析", top_k=5)

# 关闭连接
manager.close()
```

## 数据库结构

### 节点类型
- Workflow：工作流节点
- Task：任务节点
- Tool：工具节点

### 关系类型
- CONTAINS：工作流到任务的关系，带有order属性
- USES：任务到工具的关系

### 索引
- 向量索引：workflowEmbedding、taskEmbedding、toolEmbedding
- 全文索引：workflowFulltext、taskFulltext、toolFulltext
