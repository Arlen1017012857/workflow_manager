import os
import asyncio
from workflow_manager import WorkflowManager

async def test_workflow_execution():
    """Test both code execution and import execution methods"""
    
    # Initialize workflow manager
    manager = WorkflowManager(
        uri=os.getenv("NEO4J_URI", "neo4j://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "password")
    )
    
    try:
        # Create tools using code execution
        manager.create_tool(
            name="add_inline",
            description="Add two numbers using inline code",
            tool_code="""def add_inline(a, b):
    return a + b"""
        )
        
        manager.create_tool(
            name="multiply_inline",
            description="Multiply result by 2 using inline code",
            tool_code="""def multiply_inline(add_inline):
    return add_inline * 2"""
        )
        
        # Create tools using import execution from Tools directory
        manager.create_tool(
            name="add_numbers",
            description="Add two numbers using imported function",
            import_from="Tools.math_operations"
        )
        
        manager.create_tool(
            name="multiply_by_two",
            description="Multiply result by 2 using imported function",
            import_from="Tools.math_operations"
        )
        
        manager.create_tool(
            name="format_result",
            description="Format result using imported function",
            import_from="Tools.math_operations"
        )
        
        # Create tasks
        manager.create_task(
            name="add_numbers_inline",
            description="Add two numbers using inline code",
            tool_name="add_inline"
        )
        
        manager.create_task(
            name="multiply_result_inline",
            description="Multiply the result using inline code",
            tool_name="multiply_inline"
        )
        
        manager.create_task(
            name="add_numbers_imported",
            description="Add two numbers using imported function",
            tool_name="add_numbers"
        )
        
        manager.create_task(
            name="multiply_result_imported",
            description="Multiply the result using imported function",
            tool_name="multiply_by_two"
        )
        
        manager.create_task(
            name="format_result_imported",
            description="Format result using imported function",
            tool_name="format_result"
        )
        
        # Create workflow with inline code execution
        manager.create_workflow(
            name="inline_workflow",
            description="Workflow using inline code execution",
            tasks=[
                {"name": "add_numbers_inline", "order": 1},
                {"name": "multiply_result_inline", "order": 2}
            ]
        )
        
        # Create workflow with import execution
        manager.create_workflow(
            name="import_workflow",
            description="Workflow using import execution",
            tasks=[
                {"name": "add_numbers_imported", "order": 1},
                {"name": "multiply_result_imported", "order": 2},
                {"name": "format_result_imported", "order": 3}
            ]
        )
        
        # Execute workflows
        context = {'a': 5, 'b': 3}
        print("\nExecuting inline workflow...")
        result = await manager.execute_workflow("inline_workflow", context)
        print(f"Inline workflow result: {result}")
        
        print("\nExecuting import workflow...")
        result = await manager.execute_workflow("import_workflow", context)
        print(f"Import workflow result: {result}")
        
    finally:
        # Clean up
        await manager.close()

if __name__ == "__main__":
    # 使用asyncio运行异步测试函数
    asyncio.run(test_workflow_execution())
