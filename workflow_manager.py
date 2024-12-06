import os
from typing import Dict, List, Optional, Union, Any
from neo4j import GraphDatabase
import neo4j
from neo4j_graphrag.embeddings.openai import OpenAIEmbeddings
from neo4j_graphrag.retrievers import HybridCypherRetriever

class WorkflowManager:
    def __init__(self, uri: str = None, user: str = None, password: str = None, database: str = "neo4j"):
        """初始化工作流管理器"""
        # 从环境变量获取配置
        uri = uri or os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        user = user or os.getenv("NEO4J_USER", "neo4j")
        password = password or os.getenv("NEO4J_PASSWORD")
        
        if not password:
            raise ValueError("Neo4j password must be provided either through constructor or NEO4J_PASSWORD environment variable")
            
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self.embedder = OpenAIEmbeddings(
            base_url=os.getenv("EMBEDDER_BASE_URL", "http://localhost:11434/v1"),
            api_key=os.getenv("EMBEDDER_API_KEY", "ollama"),
            model=os.getenv("EMBEDDER_MODEL", "nomic-embed-text:v1.5")
        )
        
        # 初始化检索器
        self.workflow_retriever = HybridCypherRetriever(
            driver=self.driver,
            vector_index_name="workflowEmbedding",
            fulltext_index_name="workflowFulltext",
            embedder=self.embedder,
            retrieval_query="""
            MATCH (node)
            WHERE node:Workflow
            OPTIONAL MATCH (node)-[r:CONTAINS]->(task:Task)-[:USES]->(tool:Tool)
            WITH node, task, tool, r.order as task_order, score
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
            neo4j_database=database
        )
        
        self.task_retriever = HybridCypherRetriever(
            driver=self.driver,
            vector_index_name="taskEmbedding",
            fulltext_index_name="taskFulltext",
            embedder=self.embedder,
            retrieval_query="""
            MATCH (node)
            WHERE node:Task
            OPTIONAL MATCH (node)-[:USES]->(tool:Tool)
            OPTIONAL MATCH (workflow:Workflow)-[r:CONTAINS]->(node)
            WITH node, tool, workflow, r.order as task_order, score
            RETURN 
                node.name as task_name,
                node.description as task_description,
                score as similarity_score,
                tool.name as tool_name,
                collect({
                    name: workflow.name,
                    order: task_order
                }) as workflows
            """,
            neo4j_database=database
        )
        
        self.tool_retriever = HybridCypherRetriever(
            driver=self.driver,
            vector_index_name="toolEmbedding",
            fulltext_index_name="toolFulltext",
            embedder=self.embedder,
            retrieval_query="""
            MATCH (node)
            WHERE node:Tool
            OPTIONAL MATCH (task:Task)-[:USES]->(node)
            WITH node, collect(task.name) as used_by_tasks, score
            RETURN 
                node.name as tool_name,
                node.description as tool_description,
                node.function as tool_function,
                score as similarity_score,
                used_by_tasks
            """,
            neo4j_database=database
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
        """创建新任务并关联工具，如果任务已存在则返回现有任务"""
        with self.driver.session(database=self.database) as session:
            # 首先检查任务是否已存在
            existing_task = session.run("""
                MATCH (task:Task {name: $name})
                OPTIONAL MATCH (task)-[r:USES]->(tool:Tool)
                RETURN task, tool
                """,
                name=name
            ).single()
            
            if existing_task:
                return existing_task["task"]
            
            # 检查工具是否存在
            tool = session.run("""
                MATCH (tool:Tool {name: $tool_name})
                RETURN tool
                """,
                tool_name=tool_name
            ).single()
            
            if not tool:
                raise ValueError(f"Tool '{tool_name}' does not exist")
            
            # 创建新任务
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
        """创建工作流并添加任务，如果工作流已存在则返回现有工作流"""
        with self.driver.session(database=self.database) as session:
            # 检查工作流是否已存在
            existing_workflow = session.run("""
                MATCH (w:Workflow {name: $name})
                RETURN w
                """,
                name=name
            ).single()
            
            if existing_workflow:
                return existing_workflow["w"]
            
            # 检查所有任务是否存在，并收集不存在的任务
            missing_tasks = []
            for task in tasks:
                task_exists = session.run("""
                    MATCH (t:Task {name: $task_name})
                    RETURN t
                    """,
                    task_name=task["name"]
                ).single()
                
                if not task_exists:
                    missing_tasks.append(task["name"])
            
            # 如果有任务不存在，抛出异常并列出所有缺失的任务
            if missing_tasks:
                raise ValueError(f"Cannot create workflow '{name}'. The following tasks do not exist: {', '.join(missing_tasks)}")
            
            # 创建新工作流
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

    def create_tool(self, name: str, description: str, function: str) -> Dict:
        """创建新工具，如果工具已存在则返回现有工具"""
        with self.driver.session(database=self.database) as session:
            # 检查工具是否已存在
            existing_tool = session.run("""
                MATCH (tool:Tool {name: $name})
                RETURN tool
                """,
                name=name
            ).single()
            
            if existing_tool:
                return existing_tool["tool"]
            
            # 创建新工具
            embedding = self.embedder.embed_query(f"{name} {description}")
            result = session.run("""
                CREATE (tool:Tool {
                    name: $name,
                    description: $description,
                    function: $function,
                    embedding: $embedding
                })
                RETURN tool
                """,
                name=name,
                description=description,
                function=function,
                embedding=embedding
            )
            return result.single()["tool"]

    def search_workflows(self, query: str, top_k: int = 5) -> List[Dict]:
        """使用混合检索搜索工作流"""
        results = self.workflow_retriever.search(query_text=query, top_k=top_k)
        return self.parse_search_results(results, "workflow")

    def search_tasks(self, query: str, top_k: int = 5) -> List[Dict]:
        """使用混合检索搜索任务"""
        results = self.task_retriever.search(query_text=query, top_k=top_k)
        return self.parse_search_results(results, "task")

    def search_tools(self, query: str, top_k: int = 5) -> List[Dict]:
        """使用混合检索搜索工具"""
        results = self.tool_retriever.search(query_text=query, top_k=top_k)
        return self.parse_search_results(results, "tool")

    def parse_search_results(self, results, result_type: str = "workflow") -> List[Dict]:
        """解析搜索结果为字典格式
        
        Args:
            results: RetrieverResult对象
            result_type: 结果类型，可选值: "workflow", "task", "tool"
            
        Returns:
            list: 包含搜索结果信息的字典列表
        """
        parsed_results = []
        
        for item in results.items:
            content = item.content
            result_dict = {}
            
            if result_type == "workflow":
                # 解析工作流搜索结果
                workflow_name = content.split("workflow_name='")[1].split("'")[0]
                workflow_desc = content.split("workflow_description='")[1].split("'")[0]
                similarity = float(content.split("similarity_score=")[1].split(" ")[0])
                tasks_str = content.split("tasks=")[1].strip(">").strip()
                tasks = eval(tasks_str)
                
                result_dict = {
                    "name": workflow_name,
                    "description": workflow_desc,
                    "similarity_score": similarity,
                    "tasks": tasks
                }
                
            elif result_type == "task":
                # 解析任务搜索结果
                task_name = content.split("task_name='")[1].split("'")[0]
                task_desc = content.split("task_description='")[1].split("'")[0]
                similarity = float(content.split("similarity_score=")[1].split(" ")[0])
                tool_name = content.split("tool_name='")[1].split("'")[0]
                workflows_str = content.split("workflows=")[1].strip(">").strip()
                workflows = eval(workflows_str)
                
                result_dict = {
                    "name": task_name,
                    "description": task_desc,
                    "similarity_score": similarity,
                    "tool": tool_name,
                    "workflows": workflows
                }
                
            elif result_type == "tool":
                # 解析工具搜索结果
                tool_name = content.split("tool_name='")[1].split("'")[0]
                tool_desc = content.split("tool_description='")[1].split("'")[0]
                similarity = float(content.split("similarity_score=")[1].split(" ")[0])
                
                # 提取函数字符串，这里需要特殊处理因为它不是被引号包围的
                function_start = content.split("tool_function=")[1].split(" used_by_tasks=")[0].strip()
                
                # 提取used_by_tasks
                used_by_tasks_str = content.split("used_by_tasks=")[1].strip(">").strip()
                used_by_tasks = eval(used_by_tasks_str)
                
                result_dict = {
                    "name": tool_name,
                    "description": tool_desc,
                    "similarity_score": similarity,
                    "function": function_start,
                    "used_by_tasks": used_by_tasks
                }
            
            parsed_results.append(result_dict)
        
        return parsed_results

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
        uri=os.getenv("NEO4J_URI", "neo4j://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD")
    )

    # 创建任务
    task = manager.create_task(
        name="数据预处理",
        description="清洗和准备数据",
        tool_name="data_preprocessor"
    )

    # 创建工具
    tool = manager.create_tool(
        name="data_preprocessor",
        description="数据预处理工具",
        function="lambda x: {'output': x['input']}"
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