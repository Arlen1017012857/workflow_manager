from Tools.code_executor import with_context

@with_context
def add_numbers(a, b):
    """Add two numbers from context"""
    return a + b

@with_context
def multiply_by_two(add_numbers_result):
    """Multiply the result by 2"""
    return add_numbers_result * 2

@with_context
def format_result(multiply_by_two_result):
    """Format the result as a string"""
    return {'formatted': f"The final result is: {multiply_by_two_result}"}
