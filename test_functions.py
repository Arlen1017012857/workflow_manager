def add_numbers(context):
    """Add two numbers from context"""
    a = context.get('a', 0)
    b = context.get('b', 0)
    return {'result': a + b}

def multiply_by_two(context):
    """Multiply the result by 2"""
    result = context.get('result', 0)
    return {'result': result * 2}

def format_result(context):
    """Format the result as a string"""
    result = context.get('result', 0)
    return {'formatted': f"The final result is: {result}"}
