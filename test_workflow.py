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
            function="lambda context: {'result': context.get('a', 0) + context.get('b', 0)}"
        )
        
        manager.create_tool(
            name="multiply_inline",
            description="Multiply result by 2 using inline code",
            function="lambda context: {'result': context.get('result', 0) * 2}"
        )
        
        # Create tools using import execution from Tools directory
        manager.create_tool(
            name="add_imported",
            description="Add two numbers using imported function",
            function="add_numbers",
            import_from="Tools.math_operations"
        )
        
        manager.create_tool(
            name="multiply_imported",
            description="Multiply result by 2 using imported function",
            function="multiply_by_two",
            import_from="Tools.math_operations"
        )
        
        manager.create_tool(
            name="format_imported",
            description="Format result using imported function",
            function="format_result",
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
            tool_name="add_imported"
        )
        
        manager.create_task(
            name="multiply_result_imported",
            description="Multiply the result using imported function",
            tool_name="multiply_imported"
        )
        
        manager.create_task(
            name="format_result_imported",
            description="Format the result using imported function",
            tool_name="format_imported"
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
        
        # Test inline code execution
        print("\nTesting inline code execution workflow:")
        result = await manager.execute_workflow(
            workflow_name="inline_workflow",
            context_variables={"a": 5, "b": 3}
        )
        print(f"Inline execution result: {result}")
        
        # Test import execution
        print("\nTesting import execution workflow:")
        result = await manager.execute_workflow(
            workflow_name="import_workflow",
            context_variables={"a": 10, "b": 7}
        )
        print(f"Import execution result: {result}")
        
    finally:
        await manager.close()

if __name__ == "__main__":
    # 使用asyncio运行异步测试函数
    asyncio.run(test_workflow_execution())
