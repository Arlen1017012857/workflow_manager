import os
from typing import Dict, List, Optional, Union, Any
from neo4j import GraphDatabase
from openai import OpenAIEmbeddings
import numpy as np

class WorkflowManager:
    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        """初始化工作流管理器"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self.embedder = OpenAIEmbeddings(
            base_url=os.getenv("EMBEDDER_BASE_URL", "http://localhost:11434/v1"),
            api_key=os.getenv("EMBEDDER_API_KEY", "ollama"),
            model=os.getenv("EMBEDDER_MODEL", "nomic-embed-text:v1.5")
        )
        self._init_indexes()

    def _init_indexes(self):
        """初始化数据库索引"""
        with self.driver.session(database=self.database) as session:
            # 创建向量索引
            session.run("""
                CREATE VECTOR INDEX workflowEmbedding IF NOT EXISTS
                FOR (w:Workflow) ON (w.embedding)
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 1536,
                    `vector.similarity_function`: 'cosine'
                }}
            """)
            session.run("""
                CREATE VECTOR INDEX taskEmbedding IF NOT EXISTS
                FOR (t:Task) ON (t.embedding)
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 1536,
                    `vector.similarity_function`: 'cosine'
                }}
            """)
            session.run("""
                CREATE VECTOR INDEX toolEmbedding IF NOT EXISTS
                FOR (t:Tool) ON (t.embedding)
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 1536,
                    `vector.similarity_function`: 'cosine'
                }}
            """)
            
            # 创建全文索引
            session.run("""
                CREATE FULLTEXT INDEX workflowFulltext IF NOT EXISTS
                FOR (w:Workflow) ON EACH [w.name, w.description]
            """)
            session.run("""
                CREATE FULLTEXT INDEX taskFulltext IF NOT EXISTS
                FOR (t:Task) ON EACH [t.name, t.description]
            """)
            session.run("""
                CREATE FULLTEXT INDEX toolFulltext IF NOT EXISTS
                FOR (t:Tool) ON EACH [t.name, t.description]
            """)

    def create_task(self, name: str, description: str, tool_name: str) -> Dict:
        """创建新任务并关联工具"""
        with self.driver.session(database=self.database) as session:
            embedding = self.embedder.embed_query(f"{name} {description}")
            result = session.run("""
                MATCH (tool:Tool {name: $tool_name})
                CREATE (task:Task {
                    name: $name,
                    description: $description,
                    embedding: $embedding
                })
                CREATE (task)-[:USES]->(tool)
                RETURN task
                """,
                name=name,
                description=description,
                embedding=embedding,
                tool_name=tool_name
            )
            return result.single()["task"]

    def get_task(self, task_name: str) -> Dict:
        """获取任务详情"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (task:Task {name: $task_name})-[:USES]->(tool:Tool)
                RETURN task, tool
                """,
                task_name=task_name
            )
            record = result.single()
            if record:
                return {
                    "task": record["task"],
                    "tool": record["tool"]
                }
            return None

    def list_tasks(self) -> List[Dict]:
        """列出所有任务"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (task:Task)-[:USES]->(tool:Tool)
                RETURN task, tool
                """)
            return [{
                "task": record["task"],
                "tool": record["tool"]
            } for record in result]

    def delete_task(self, task_name: str) -> bool:
        """删除未使用的任务"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (task:Task {name: $task_name})
                WHERE NOT (task)<-[:CONTAINS]-()
                DETACH DELETE task
                RETURN count(task) as deleted
                """,
                task_name=task_name
            )
            return result.single()["deleted"] > 0

    def create_workflow(self, name: str, description: str, tasks: List[Dict[str, Union[str, int]]]) -> Dict:
        """创建工作流并添加任务"""
        with self.driver.session(database=self.database) as session:
            embedding = self.embedder.embed_query(f"{name} {description}")
            result = session.run("""
                CREATE (w:Workflow {
                    name: $name,
                    description: $description,
                    embedding: $embedding
                })
                RETURN w
                """,
                name=name,
                description=description,
                embedding=embedding
            )
            workflow = result.single()["w"]
            
            # 添加任务到工作流
            for task in tasks:
                self.add_task_to_workflow(
                    workflow_name=name,
                    task_name=task["name"],
                    order=task["order"]
                )
            return workflow

    def add_task_to_workflow(self, workflow_name: str, task_name: str, order: int) -> bool:
        """将任务添加到工作流"""
        with self.driver.session(database=self.database) as session:
            # 先更新现有任务的顺序
            session.run("""
                MATCH (w:Workflow {name: $workflow_name})-[r:CONTAINS]->(t:Task)
                WHERE r.order >= $order
                SET r.order = r.order + 1
                """,
                workflow_name=workflow_name,
                order=order
            )
            
            # 添加新任务
            result = session.run("""
                MATCH (w:Workflow {name: $workflow_name}), (t:Task {name: $task_name})
                CREATE (w)-[r:CONTAINS {order: $order}]->(t)
                RETURN r
                """,
                workflow_name=workflow_name,
                task_name=task_name,
                order=order
            )
            return result.single() is not None

    def remove_task_from_workflow(self, workflow_name: str, task_name: str) -> bool:
        """从工作流中移除任务"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (w:Workflow {name: $workflow_name})-[r:CONTAINS]->(t:Task {name: $task_name})
                DELETE r
                WITH w, r.order as removed_order
                MATCH (w)-[r2:CONTAINS]->(t2:Task)
                WHERE r2.order > removed_order
                SET r2.order = r2.order - 1
                RETURN count(r) as removed
                """,
                workflow_name=workflow_name,
                task_name=task_name
            )
            return result.single()["removed"] > 0

    def search_workflows(self, query: str, top_k: int = 5) -> List[Dict]:
        """使用混合检索搜索工作流"""
        with self.driver.session(database=self.database) as session:
            embedding = self.embedder.embed_query(query)
            result = session.run("""
                CALL db.index.vector.queryNodes('workflowEmbedding', $top_k, $embedding)
                YIELD node, score
                MATCH (node)-[r:CONTAINS]->(task:Task)-[:USES]->(tool:Tool)
                WITH node, score, task, tool, r.order as task_order
                ORDER BY node.name, task_order
                RETURN 
                    node.name as workflow_name,
                    node.description as workflow_description,
                    score as similarity_score,
                    collect({
                        name: task.name,
                        description: task.description,
                        order: task_order,
                        tool: tool.name
                    }) as tasks
                """,
                embedding=embedding,
                top_k=top_k
            )
            return [dict(record) for record in result]

    def search_tasks(self, query: str, top_k: int = 5) -> List[Dict]:
        """使用混合检索搜索任务"""
        with self.driver.session(database=self.database) as session:
            embedding = self.embedder.embed_query(query)
            result = session.run("""
                CALL db.index.vector.queryNodes('taskEmbedding', $top_k, $embedding)
                YIELD node, score
                MATCH (node)-[:USES]->(tool:Tool)
                OPTIONAL MATCH (workflow:Workflow)-[r:CONTAINS]->(node)
                RETURN 
                    node.name as task_name,
                    node.description as task_description,
                    score as similarity_score,
                    tool.name as tool_name,
                    collect({
                        name: workflow.name,
                        order: r.order
                    }) as workflows
                """,
                embedding=embedding,
                top_k=top_k
            )
            return [dict(record) for record in result]

    def search_tools(self, query: str, top_k: int = 5) -> List[Dict]:
        """使用混合检索搜索工具"""
        with self.driver.session(database=self.database) as session:
            embedding = self.embedder.embed_query(query)
            result = session.run("""
                CALL db.index.vector.queryNodes('toolEmbedding', $top_k, $embedding)
                YIELD node, score
                OPTIONAL MATCH (task:Task)-[:USES]->(node)
                RETURN 
                    node.name as tool_name,
                    node.description as tool_description,
                    node.function as tool_function,
                    score as similarity_score,
                    collect(task.name) as used_by_tasks
                """,
                embedding=embedding,
                top_k=top_k
            )
            return [dict(record) for record in result]

    def execute_workflow(self, workflow_name: str, context_variables: Dict[str, Any] = None) -> Dict[str, Any]:
        """执行工作流"""
        if context_variables is None:
            context_variables = {}
            
        with self.driver.session(database=self.database) as session:
            # 获取工作流中的所有任务，按顺序排列
            result = session.run("""
                MATCH (w:Workflow {name: $workflow_name})-[r:CONTAINS]->(task:Task)-[:USES]->(tool:Tool)
                RETURN task, tool, r.order as order
                ORDER BY r.order
                """,
                workflow_name=workflow_name
            )
            
            tasks = [(record["task"], record["tool"], record["order"]) for record in result]
            
            # 按顺序执行每个任务
            for task, tool, order in tasks:
                try:
                    # 获取工具函数
                    tool_function = eval(tool["function"])
                    # 执行工具函数，传入上下文变量
                    result = tool_function(context_variables)
                    # 更新上下文变量
                    context_variables.update(result)
                except Exception as e:
                    return {
                        "success": False,
                        "error": f"Error executing task {task['name']}: {str(e)}",
                        "context": context_variables
                    }
            
            return {
                "success": True,
                "context": context_variables
            }

    def close(self):
        """关闭数据库连接"""
        self.driver.close()


if __name__ == "__main__":
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