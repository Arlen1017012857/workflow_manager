import os
from typing import Dict, List, Optional, Union, Any
from neo4j import GraphDatabase
import neo4j
from neo4j_graphrag.embeddings.openai import OpenAIEmbeddings
from neo4j_graphrag.retrievers import HybridCypherRetriever
from Tools.code_executor import CodeExecutor

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
        
        # 初始化代码执行器
        self.code_executor = CodeExecutor(max_kernels=3)
        
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
                toString(node.tool_code) as tool_code,
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

    def create_tool(self, name: str, description: str, tool_code: str = None, import_from: Optional[str] = None):
        """创建新工具，如果工具已存在则返回已存在的工具
        
        Args:
            name: 工具名称
            description: 工具描述
            tool_code: 工具代码
            import_from: 函数导入路径，例如 'module.submodule'
        """
        with self.driver.session(database=self.database) as session:
            # 检查工具是否已存在
            existing_tool = session.run("""
                MATCH (tool:Tool {name: $name})
                RETURN tool
                LIMIT 1
                """,
                name=name
            ).single()
            
            if existing_tool:
                return existing_tool["tool"]
            
            # 创建新的嵌入向量
            embedding = self.embedder.embed_query(f"{name} {description}")
            
            # 创建新工具
            result = session.run("""
                CREATE (tool:Tool {
                    name: $name,
                    description: $description,
                    tool_code: $tool_code,
                    import_from: $import_from,
                    embedding: $embedding
                })
                RETURN tool
                LIMIT 1
                """,
                name=name,
                description=description,
                tool_code=tool_code,
                import_from=import_from,
                embedding=embedding
            )
            
            record = result.single()
            return record["tool"] if record else None

    def update_tool(self, name: str, description: str = None, tool_code: str = None, import_from: Optional[str] = None):
        """更新现有工具的属性
        
        Args:
            name: 工具名称
            description: 工具描述
            tool_code: 工具代码
            import_from: 函数导入路径，例如 'module.submodule'
        """
        with self.driver.session(database=self.database) as session:
            # 检查工具是否存在
            exists = session.run("""
                MATCH (tool:Tool {name: $name})
                RETURN count(tool) > 0 as exists
                """,
                name=name
            ).single()["exists"]
            
            if not exists:
                raise ValueError(f"Tool '{name}' does not exist. Use create_tool to create new tools.")
            
            # 创建新的嵌入向量
            embedding = self.embedder.embed_query(f"{name} {description if description else ''}")
            
            # 更新工具属性
            result = session.run("""
                MATCH (tool:Tool {name: $name})
                SET tool.embedding = $embedding
                SET tool.description = CASE WHEN $description IS NULL THEN tool.description ELSE $description END
                SET tool.tool_code = CASE WHEN $tool_code IS NULL THEN tool.tool_code ELSE $tool_code END
                SET tool.import_from = CASE WHEN $import_from IS NULL THEN tool.import_from ELSE $import_from END
                RETURN tool
                LIMIT 1
                """,
                name=name,
                description=description,
                tool_code=tool_code,
                import_from=import_from,
                embedding=embedding
            )
            
            record = result.single()
            return record["tool"] if record else None

    def create_task(self, name: str, description: str, tool_name: str) -> Dict:
        """创建新任务并关联工具，如果任务已存在则返回已存在的任务
        
        Args:
            name: 任务名称
            description: 任务描述
            tool_name: 工具名称
        """
        with self.driver.session(database=self.database) as session:
            # 检查任务是否已存在
            existing_task = session.run("""
                MATCH (task:Task {name: $name})-[:USES]->(tool:Tool)
                RETURN task, tool
                LIMIT 1
                """,
                name=name
            ).single()
            
            if existing_task:
                return existing_task["task"]
            
            # 检查工具是否存在
            tool = session.run("""
                MATCH (tool:Tool {name: $tool_name})
                RETURN tool
                LIMIT 1
                """,
                tool_name=tool_name
            ).single()
            
            if not tool:
                raise ValueError(f"Tool '{tool_name}' does not exist")
            
            # 创建新的嵌入向量
            embedding = self.embedder.embed_query(f"{name} {description}")
            
            # 创建新任务并关联工具
            result = session.run("""
                MATCH (tool:Tool {name: $tool_name})
                CREATE (task:Task {
                    name: $name,
                    description: $description,
                    embedding: $embedding
                })
                CREATE (task)-[:USES]->(tool)
                RETURN task
                LIMIT 1
                """,
                name=name,
                description=description,
                tool_name=tool_name,
                embedding=embedding
            )
            
            record = result.single()
            return record["task"] if record else None

    def update_task(self, name: str, description: str = None, tool_name: str = None) -> Dict:
        """更新现有任务的属性和关联工具
        
        Args:
            name: 任务名称
            description: 任务描述
            tool_name: 工具名称
        """
        with self.driver.session(database=self.database) as session:
            # 检查任务是否存在
            exists = session.run("""
                MATCH (task:Task {name: $name})
                RETURN count(task) > 0 as exists
                """,
                name=name
            ).single()["exists"]
            
            if not exists:
                raise ValueError(f"Task '{name}' does not exist. Use create_task to create new tasks.")
            
            # 如果指定了新工具，检查工具是否存在
            if tool_name:
                tool = session.run("""
                    MATCH (tool:Tool {name: $tool_name})
                    RETURN tool
                    LIMIT 1
                    """,
                    tool_name=tool_name
                ).single()
                
                if not tool:
                    raise ValueError(f"Tool '{tool_name}' does not exist")
            
            # 创建新的嵌入向量
            embedding = self.embedder.embed_query(f"{name} {description if description else ''}")
            
            # 更新任务属性和关联工具
            result = session.run("""
                MATCH (task:Task {name: $name})
                SET task.embedding = $embedding
                SET task.description = CASE WHEN $description IS NULL THEN task.description ELSE $description END
                
                WITH task
                OPTIONAL MATCH (task)-[r:USES]->(:Tool)
                WHERE $tool_name IS NOT NULL
                DELETE r
                
                WITH task
                MATCH (tool:Tool {name: CASE WHEN $tool_name IS NULL THEN task.tool_name ELSE $tool_name END})
                MERGE (task)-[:USES]->(tool)
                
                RETURN task
                LIMIT 1
                """,
                name=name,
                description=description,
                tool_name=tool_name,
                embedding=embedding
            )
            
            record = result.single()
            return record["task"] if record else None

    def create_workflow(self, name: str, description: str, tasks: List[Dict[str, Union[str, int]]]) -> Dict:
        """创建新工作流并添加任务，如果工作流已存在则返回已存在的工作流
        
        Args:
            name: 工作流名称
            description: 工作流描述
            tasks: 任务列表，每个任务包含 name 和 order
        """
        with self.driver.session(database=self.database) as session:
            # 检查工作流是否已存在
            existing_workflow = session.run("""
                MATCH (w:Workflow {name: $name})
                RETURN w
                LIMIT 1
                """,
                name=name
            ).single()
            
            if existing_workflow:
                return existing_workflow["w"]
            
            # 检查所有任务是否存在
            missing_tasks = []
            for task in tasks:
                task_exists = session.run("""
                    MATCH (t:Task {name: $task_name})
                    RETURN count(t) > 0 as exists
                    """,
                    task_name=task["name"]
                ).single()["exists"]
                
                if not task_exists:
                    missing_tasks.append(task["name"])
            
            if missing_tasks:
                raise ValueError(f"Cannot create workflow '{name}'. The following tasks do not exist: {', '.join(missing_tasks)}")
            
            # 创建新的嵌入向量
            embedding = self.embedder.embed_query(f"{name} {description}")
            
            # 创建新工作流并添加任务
            result = session.run("""
                CREATE (w:Workflow {
                    name: $name,
                    description: $description,
                    embedding: $embedding
                })
                
                WITH w
                UNWIND $tasks as task
                MATCH (t:Task {name: task.name})
                CREATE (w)-[r:CONTAINS {order: task.order}]->(t)
                
                RETURN w
                LIMIT 1
                """,
                name=name,
                description=description,
                embedding=embedding,
                tasks=tasks
            )
            
            record = result.single()
            return record["w"] if record else None

    def update_workflow(self, name: str, description: str = None, tasks: List[Dict[str, Union[str, int]]] = None) -> Dict:
        """更新现有工作流的属性和任务
        
        Args:
            name: 工作流名称
            description: 工作流描述
            tasks: 任务列表，每个任务包含 name 和 order
        """
        with self.driver.session(database=self.database) as session:
            # 检查工作流是否存在
            exists = session.run("""
                MATCH (w:Workflow {name: $name})
                RETURN count(w) > 0 as exists
                """,
                name=name
            ).single()["exists"]
            
            if not exists:
                raise ValueError(f"Workflow '{name}' does not exist. Use create_workflow to create new workflows.")
            
            # 如果指定了新任务列表，检查所有任务是否存在
            if tasks:
                missing_tasks = []
                for task in tasks:
                    task_exists = session.run("""
                        MATCH (t:Task {name: $task_name})
                        RETURN count(t) > 0 as exists
                        """,
                        task_name=task["name"]
                    ).single()["exists"]
                    
                    if not task_exists:
                        missing_tasks.append(task["name"])
                
                if missing_tasks:
                    raise ValueError(f"Cannot update workflow '{name}'. The following tasks do not exist: {', '.join(missing_tasks)}")
            
            # 创建新的嵌入向量
            embedding = self.embedder.embed_query(f"{name} {description if description else ''}")
            
            # 更新工作流属性
            result = session.run("""
                MATCH (w:Workflow {name: $name})
                SET w.embedding = $embedding
                SET w.description = CASE WHEN $description IS NULL THEN w.description ELSE $description END
                
                WITH w
                OPTIONAL MATCH (w)-[r:CONTAINS]->(:Task)
                WHERE $tasks IS NOT NULL
                DELETE r
                
                WITH w
                UNWIND CASE WHEN $tasks IS NULL THEN [] ELSE $tasks END as task
                MATCH (t:Task {name: task.name})
                CREATE (w)-[r:CONTAINS {order: task.order}]->(t)
                
                RETURN w
                LIMIT 1
                """,
                name=name,
                description=description,
                embedding=embedding,
                tasks=tasks
            )
            
            record = result.single()
            return record["w"] if record else None

    def get_task(self, task_name: str) -> Dict:
        """获取任务详情"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (task:Task {name: $task_name})-[:USES]->(tool:Tool)
                RETURN task, tool
                LIMIT 1
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
                
                # 提取tool_code - 处理双引号包裹的情况
                tool_code_start = content.split('tool_code="')[1]
                tool_code_end = tool_code_start.split('" similarity_score')[0]
                tool_code = tool_code_end.replace('\\"', '"')  # 处理可能的转义引号
                
                # 提取used_by_tasks
                used_by_tasks_str = content.split("used_by_tasks=")[1].strip(">").strip()
                used_by_tasks = eval(used_by_tasks_str)
                
                result_dict = {
                    "name": tool_name,
                    "description": tool_desc,
                    "similarity_score": similarity,
                    "tool_code": tool_code,
                    "used_by_tasks": used_by_tasks
                }
            
            parsed_results.append(result_dict)
        
        return parsed_results

    async def execute_task_by_code(self, task: Dict, tool: Dict, context_variables: Dict[str, Any]) -> Dict[str, Any]:
        """使用代码执行方式执行任务"""
        try:
            print(f"Executing task: {task['name']}")
            if not tool.get("tool_code"):
                raise ValueError(f"Tool {tool['name']} does not have tool_code")
            
            # 获取内核ID
            kernel_id = await self.code_executor.start_kernel()
            if not kernel_id:
                raise RuntimeError("Failed to start kernel")
            
            # 构建执行代码
            code = f"""
# 导入装饰器
import sys

sys.path.append(".")
from Tools.code_executor import with_context

# 定义上下文
context = {context_variables}

{tool['tool_code']}

# 装饰函数并执行
result = with_context({tool['name']})(context)
result  # 返回结果
"""
            
            # 执行代码
            output, execution_time, kernel_id = await self.code_executor.execute_code(
                code=code,
                kernel_id=kernel_id,
                show_code=True
            )
            
            # 检查是否有错误输出
            if isinstance(output, str) and ("Error:" in output or "Exception:" in output):
                return {
                    "success": False,
                    "error": output,
                    "task": task["name"],
                    "context": context_variables
                }
            
            try:
                # 解析输出
                if output is None:
                    result = {}
                else:
                    # 尝试解析输出为Python对象
                    result = eval(output)
                    
                    # 如果结果不是字典，使用工具名作为键
                    if not isinstance(result, dict):
                        result = {tool['name']: result}
                
                # 更新上下文
                context_variables.update(result)
                
                return {
                    "success": True,
                    "context": context_variables
                }
                
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to parse output: {str(e)}",
                    "raw_output": output,
                    "task": task["name"],
                    "context": context_variables
                }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "task": task["name"],
                "context": context_variables
            }

    async def execute_task_by_import(self, task: Dict, tool: Dict, context_variables: Dict[str, Any]) -> Dict[str, Any]:
        """使用动态导入方式执行任务"""
        try:
            if not tool.get("import_from"):
                raise ValueError(f"Tool {tool['name']} does not have import_from attribute")
            
            # 使用importlib动态导入模块和函数
            import importlib
            module = importlib.import_module(tool['import_from'])
            
            # 如果没有指定 tool_code，使用与工具同名的函数
            function_name = tool.get('tool_code') or tool['name']
            func = getattr(module, function_name)
            
            # 执行函数
            result = func(context_variables)
            
            # 处理返回结果
            if result is None:
                result = {}
            elif not isinstance(result, dict):
                result = {f"{function_name}_result": result}
                
            context_variables.update(result)
            
            return {
                "success": True,
                "context": context_variables
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "task": task["name"],
                "context": context_variables
            }

    async def execute_workflow(self, workflow_name: str, context_variables: Dict[str, Any] = None) -> Dict[str, Any]:
        """执行工作流
        
        根据工具的tool_code属性是否为空来选择执行方式：
        - tool_code不为空：使用代码执行方式
        - tool_code为空：使用动态导入方式
        
        Args:
            workflow_name: 工作流名称
            context_variables: 上下文变量
            
        Returns:
            Dict[str, Any]: 执行结果和上下文变量
        """
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
            
            # 显示内核统计信息
            # self.code_executor.print_kernel_stats()
            
            # 按顺序执行每个任务
            for task, tool, order in tasks:
                # 根据tool_code属性选择执行方式
                if tool.get("tool_code") and tool["tool_code"].strip():
                    result = await self.execute_task_by_code(task, tool, context_variables)
                else:
                    result = await self.execute_task_by_import(task, tool, context_variables)
                print(result)
                # 检查执行结果
                if not result["success"]:
                    return result
                
                # 更新上下文变量
                context_variables = result["context"]
                
                # 显示内核统计信息
                # self.code_executor.print_kernel_stats()
            
            return {
                "success": True,
                "context": context_variables
            }

    async def close(self):
        """关闭数据库连接和清理资源"""
        if hasattr(self, 'driver'):
            self.driver.close()
        if hasattr(self, 'code_executor'):
            await self.code_executor.cleanup()


